package utils

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"google.golang.org/grpc"
)

const (
	DenyLightningUpdateFrequency = 5
)

func (mgr *StoreManager) GetAllStores(ctx context.Context) ([]*metapb.Store, error) {
	return mgr.PDClient().GetAllStores(ctx)
}

func (mgr *StoreManager) GetDenyLightningClient(ctx context.Context, storeID uint64) (DenyLightningClient, error) {
	var cli import_sstpb.ImportSSTClient
	err := mgr.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = import_sstpb.NewImportSSTClient(cc)
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

type DenyImportingEnv interface {
	GetAllStores(ctx context.Context) ([]*metapb.Store, error)
	GetDenyLightningClient(ctx context.Context, storeID uint64) (DenyLightningClient, error)
}

type DenyLightningClient interface {
	// Temporarily disable ingest / download / write for data listeners don't support catching import data.
	DenyImportRPC(ctx context.Context, in *import_sstpb.DenyImportRPCRequest, opts ...grpc.CallOption) (*import_sstpb.DenyImportRPCResponse, error)
}

type DenyImporting struct {
	env  DenyImportingEnv
	name string
}

func NewDenyImporting(name string, env DenyImportingEnv) *DenyImporting {
	return &DenyImporting{
		env:  env,
		name: name,
	}
}

// DenyAllStore tries to deny all current stores' lightning execution for the period of time.
// Returns a map mapping store ID to whether they are already denied to import tasks.
func (d *DenyImporting) DenyAllStore(ctx context.Context, dur time.Duration) (map[uint64]bool, error) {
	return d.forEachStores(ctx, func() *import_sstpb.DenyImportRPCRequest {
		return &import_sstpb.DenyImportRPCRequest{
			ShouldDenyImports: true,
			DurationSecs:      uint64(dur.Seconds()),
			Caller:            d.name,
		}
	})
}

func (d *DenyImporting) AllowAllStores(ctx context.Context) (map[uint64]bool, error) {
	return d.forEachStores(ctx, func() *import_sstpb.DenyImportRPCRequest {
		return &import_sstpb.DenyImportRPCRequest{
			ShouldDenyImports: false,
			Caller:            d.name,
		}
	})
}

// forEachStores send the request to each stores reachable.
// Returns a map mapping store ID to whether they are already denied to import tasks.
func (d *DenyImporting) forEachStores(ctx context.Context, makeReq func() *import_sstpb.DenyImportRPCRequest) (map[uint64]bool, error) {
	stores, err := d.env.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "failed to get all stores")
	}

	result := map[uint64]bool{}
	for _, store := range stores {
		cli, err := d.env.GetDenyLightningClient(ctx, store.Id)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to get client for store %d", store.Id)
		}
		req := makeReq()
		resp, err := cli.DenyImportRPC(ctx, req)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to deny lightning rpc for store %d", store.Id)
		}
		result[store.Id] = resp.AlreadyDeniedImports
	}
	return result, nil
}

// HasKeptDenying checks whether a result returned by `DenyAllStores` is able to keep the consistency with last request.
// i.e. Whether the store has some holes of pausing the import requests.
func (d *DenyImporting) ConsistentWithPrev(result map[uint64]bool) error {
	for storeId, denied := range result {
		if !denied {
			return errors.Annotatef(berrors.ErrPossibleInconsistency, "failed to keep importing to store %d being denied, the state might be inconsistency", storeId)
		}
	}
	return nil
}

func (d *DenyImporting) Keeper(ctx context.Context, ttl time.Duration) error {
	t := time.NewTicker(ttl / DenyLightningUpdateFrequency)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			res, err := d.DenyAllStore(ctx, ttl)
			if err != nil {
				return err
			}
			if err := d.ConsistentWithPrev(res); err != nil {
				return err
			}
		}
	}
}
