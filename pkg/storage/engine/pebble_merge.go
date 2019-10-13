// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// mergeTimeSeries combines two `InternalTimeSeriesData`s and returns the result as an
// `InternalTimeSeriesData`.
func mergeTimeSeries(oldTs, newTs roachpb.InternalTimeSeriesData) (roachpb.InternalTimeSeriesData, error) {
	if oldTs.StartTimestampNanos != newTs.StartTimestampNanos {
		return roachpb.InternalTimeSeriesData{}, errors.Errorf("start timestamp mismatch")
	}
	return roachpb.InternalTimeSeriesData{}, nil
}

// mergeTimeSeriesRaw combines two serialized `InternalTimeSeriesData`s and returns the result
// as a serialized `InternalTimeSeriesData`.
func mergeTimeSeriesRaw(oldTsBytes, newTsBytes []byte) ([]byte, error) {
	var oldTs, newTs, mergedTs roachpb.InternalTimeSeriesData
	if err := protoutil.Unmarshal(oldTsBytes, &oldTs); err != nil {
		return nil, errors.Errorf("corrupted old timeseries: %v", err)
	}
	if err := protoutil.Unmarshal(newTsBytes, &newTs); err != nil {
		return nil, errors.Errorf("corrupted new timeseries: %v", err)
	}

	var err error
	if mergedTs, err = mergeTimeSeries(oldTs, newTs); err != nil {
		return nil, errors.Errorf("mergeTimeSeries: %v", err)
	}

	if res, err := protoutil.Marshal(&mergedTs); err != nil {
		return nil, errors.Errorf("corrupted merged timeseries: %v", err)
	} else {
		return res, nil
	}
}

// Merge combines two serialized `MVCCMetadata`s and returns the result as a serialized
// `MVCCMetadata`.
func Merge(key, oldValue, newValue, buf []byte) []byte {
	var oldMeta, newMeta, mergedMeta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(oldValue, &oldMeta); err != nil {
		return []byte(fmt.Sprintf("corrupted old mvcc: %v", err))
	}
	if err := protoutil.Unmarshal(newValue, &newMeta); err != nil {
		return []byte(fmt.Sprintf("corrupted new mvcc: %v", err))
	}

	if tsBytes, err := mergeTimeSeriesRaw(oldMeta.RawBytes, newMeta.RawBytes); err != nil {
		return []byte(fmt.Sprintf("mergeTimeSeriesRaw: %v", err))
	} else {
		mergedMeta.RawBytes = tsBytes
	}

	if res, err := protoutil.Marshal(&mergedMeta); err != nil {
		return []byte(fmt.Sprintf("corrupted merged mvcc: %v", err))
	} else {
		return res
	}
}
