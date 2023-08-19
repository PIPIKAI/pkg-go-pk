package pketcd

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdClient struct {
	client *clientv3.Client
	Kv     clientv3.KV
	prefix string
}

func NewEtcdClient(config clientv3.Config) (*EtcdClient, error) {
	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	return &EtcdClient{client: cli, Kv: clientv3.KV(cli)}, nil
}

func (cli *EtcdClient) SetNx(ctx context.Context, key, value string, ops ...clientv3.OpOption) (int, error) {
	txnRes, err := cli.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", "0")).
		Then(clientv3.OpPut(key, value, ops...)).
		Commit()
	if err != nil {
		return 0, err
	}
	if txnRes.Succeeded {
		return 1, nil
	}
	return 0, nil
}

func (cli *EtcdClient) Locker(ctx context.Context, lockKey string, leaseTtl int64) (bool, error) {
	if lockKey == "" {
		return false, fmt.Errorf("lockKey is null")
	}
	lease := clientv3.NewLease(cli.client)
	grant, err := lease.Grant(ctx, leaseTtl)
	if err != nil {
		return false, fmt.Errorf("Grant lease err:%v", err)
	}
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())
	keepRespChan, err := lease.KeepAlive(ctx, grant.ID)
	if err != nil {
		return false, fmt.Errorf("KeepAlive err:%v", err)
	}
	// 续约应答
	go func() {
		for {
			select {
			case keepResp := <-keepRespChan:
				if keepResp == nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	resp, err := cli.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, lockValue, clientv3.WithLease(grant.ID))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("put txn err:%v", err)
	}
	if resp.Succeeded {
		return true, nil
	}
	return false, nil
}

func (cli *EtcdClient) Unlocker(ctx context.Context, lockKey string) error {
	_, err := cli.client.Delete(ctx, lockKey)
	if err != nil {
		return err
	}
	return nil
}

func (cli *EtcdClient) SvcRegister(ctx context.Context, prefixKey string, value string, leaseTtl int64) error {
	grant, err := cli.client.Grant(ctx, leaseTtl)
	if err != nil {
		return err
	}
	_, err = cli.client.Put(ctx, prefixKey, value, clientv3.WithLease(grant.ID))
	if err != nil {
		return err
	}
	go cli.keepAliveChan(ctx, grant.ID)
	return nil
}

func (cli *EtcdClient) keepAliveChan(ctx context.Context, leaseID clientv3.LeaseID) {
	lease := clientv3.NewLease(cli.client)
	// 进行持续的续约
	leaseChan, err := lease.KeepAlive(ctx, leaseID)
	if err != nil {
		log.Fatal("lease register keepAlive faith... err: ", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _ = <-leaseChan:
		}
	}
}

func (cli *EtcdClient) SvcDiscover(ctx context.Context, prefixKey string, whenGet func(string, string), whenDel func(string, string)) error {
	kvCli := clientv3.KV(cli.client)
	kvRes, err := kvCli.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for idx := range kvRes.Kvs {
		kv := kvRes.Kvs[idx]
		whenGet(string(kv.Key), string(kv.Value))
	}

	watcher := clientv3.NewWatcher(cli.client)
	watchChan := watcher.Watch(context.Background(), prefixKey, clientv3.WithPrefix())

	go func() {
		for watchResponse := range watchChan {
			for _, e := range watchResponse.Events {
				switch e.Type {
				// [新增 | 修改]
				case clientv3.EventTypePut:
					whenGet(string(e.Kv.Key), string(e.Kv.Value))
					// 删除
				case clientv3.EventTypeDelete:
					whenDel(string(e.Kv.Key), string(e.Kv.Value))
				}
			}
		}
	}()
	return nil
}
