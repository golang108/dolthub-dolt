// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogreplication

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

type registeredReplica struct {
	connectionId uint32
	host         string
	port         uint16
}

// doltBinlogPrimaryController implements the binlogreplication.BinlogPrimaryController
// interface from GMS and is the main extension point where Dolt plugs in to GMS and
// interprets commands and statements related to serving binlog events.
type doltBinlogPrimaryController struct {
	registeredReplicas []*registeredReplica
	streamerManager    *binlogStreamerManager
	BinlogProducer     *binlogProducer
}

var _ binlogreplication.BinlogPrimaryController = (*doltBinlogPrimaryController)(nil)

// NewDoltBinlogPrimaryController creates a new doltBinlogPrimaryController instance.
func NewDoltBinlogPrimaryController() *doltBinlogPrimaryController {
	controller := doltBinlogPrimaryController{
		registeredReplicas: make([]*registeredReplica, 0),
		streamerManager:    newBinlogStreamerManager(),
	}
	return &controller
}

func (d *doltBinlogPrimaryController) StreamerManager() *binlogStreamerManager {
	return d.streamerManager
}

// RegisterReplica implements the BinlogPrimaryController interface.
func (d *doltBinlogPrimaryController) RegisterReplica(ctx *sql.Context, c *mysql.Conn, replicaHost string, replicaPort uint16) error {
	if d.BinlogProducer == nil {
		return fmt.Errorf("no binlog currently being recorded; make sure the server is started with @@log_bin enabled")
	}

	// TODO: Do we actually need the connection here? Doesn't seem like it...
	// TODO: Obviously need locking on the datastructure, but just getting something stubbed out
	d.registeredReplicas = append(d.registeredReplicas, &registeredReplica{
		connectionId: c.ConnectionID,
		host:         replicaHost,
		port:         replicaPort,
	})

	return nil
}

// BinlogDumpGtid implements the BinlogPrimaryController interface.
func (d *doltBinlogPrimaryController) BinlogDumpGtid(ctx *sql.Context, conn *mysql.Conn, gtidSet mysql.GTIDSet) error {
	if d.BinlogProducer == nil {
		return fmt.Errorf("no binlog currently being recorded; make sure the server is started with @@log_bin enabled")
	}

	// TODO: Is this the right/best way to get the binlogformat and binlog stream?
	err := d.streamerManager.StartStream(ctx, conn, d.BinlogProducer.binlogFormat, d.BinlogProducer.binlogStream)
	if err != nil {
		logrus.Warnf("exiting binlog streamer due to error: %s", err.Error())
	} else {
		logrus.Trace("exiting binlog streamer cleanly")
	}

	return err
}

// ListReplicas implements the BinlogPrimaryController interface.
func (d *doltBinlogPrimaryController) ListReplicas(ctx *sql.Context) error {
	return fmt.Errorf("ListReplicas not implemented in Dolt yet")
}

// ListBinaryLogs implements the BinlogPrimaryController interface.
func (d *doltBinlogPrimaryController) ListBinaryLogs(ctx *sql.Context) error {
	return fmt.Errorf("ListBinaryLogs not implemented in Dolt yet")
}

// GetBinaryLogStatus implements the BinlogPrimaryController interface.
func (d *doltBinlogPrimaryController) GetBinaryLogStatus(ctx *sql.Context) ([]binlogreplication.BinaryLogStatus, error) {
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: This data is just stubbed out; need to fill in the correct GTID info
	return []binlogreplication.BinaryLogStatus{{
		File:          binlogFilename,
		Position:      uint(d.BinlogProducer.binlogStream.LogPosition),
		DoDbs:         "",
		IgnoreDbs:     "",
		ExecutedGtids: serverUuid + ":1-3",
	}}, nil
}