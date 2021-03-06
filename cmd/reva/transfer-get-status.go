// Copyright 2018-2021 CERN
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
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package main

import (
	"errors"
	"io"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	datatx "github.com/cs3org/go-cs3apis/cs3/tx/v1beta1"
)

func transferGetStatusCommand() *command {
	cmd := newCommand("transfer-get-status")
	cmd.Description = func() string { return "get the status of a transfer" }
	cmd.Usage = func() string { return "Usage: transfer-get-status [-flags]" }
	txID := cmd.String("txID", "", "the transfer identifier")

	cmd.Action = func(w ...io.Writer) error {
		// validate flags
		if *txID == "" {
			return errors.New("txID must be specified: use -txID flag\n" + cmd.Usage())
		}

		ctx := getAuthContext()
		client, err := getClient()
		if err != nil {
			return err
		}

		getStatusRequest := &datatx.GetTransferStatusRequest{}

		getStatusResponse, err := client.GetTransferStatus(ctx, getStatusRequest)
		if err != nil {
			return err
		}
		if getStatusResponse.Status.Code != rpc.Code_CODE_OK {
			return formatError(getStatusResponse.Status)
		}

		return nil
	}
	return cmd
}
