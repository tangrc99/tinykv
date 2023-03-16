package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		resp.NotFound = true
		return resp, err
	}

	value, err := reader.GetCF(req.GetCf(), req.GetKey())

	if err != nil {
		resp.Error = err.Error()
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = value
	if resp.Value == nil {
		resp.NotFound = true
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	p := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}

	err := server.storage.Write(req.Context, []storage.Modify{{Data: p}})

	resp := &kvrpcpb.RawPutResponse{}

	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	p := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}

	err := server.storage.Write(req.Context, []storage.Modify{{Data: p}})

	resp := &kvrpcpb.RawDeleteResponse{}

	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	iter := reader.IterCF(req.GetCf())

	for iter.Seek(req.StartKey); iter.Valid() && len(resp.Kvs) < int(req.Limit); iter.Next() {
		pair := &kvrpcpb.KvPair{
			Key: iter.Item().Key(),
		}
		value, err := iter.Item().Value()

		if value == nil || len(value) == 0 || err != nil {
			continue
		}
		pair.Value = value
		resp.Kvs = append(resp.Kvs, pair)
	}

	return resp, nil

}
