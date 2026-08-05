// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package cloud

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
	as3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type listObjectsV2Stub struct {
	pages   []*as3.ListObjectsV2Output
	errAt   int
	pageErr error
	tokens  []string
	calls   int
}

func (stub *listObjectsV2Stub) ListObjectsV2(_ context.Context, input *as3.ListObjectsV2Input,
	_ ...func(*as3.Options)) (*as3.ListObjectsV2Output, error) {
	token := ""
	if nil != input.ContinuationToken {
		token = *input.ContinuationToken
	}
	stub.tokens = append(stub.tokens, token)
	call := stub.calls
	stub.calls++
	if call == stub.errAt {
		return nil, stub.pageErr
	}
	return stub.pages[call], nil
}

func TestListS3ObjectsPagination(t *testing.T) {
	firstPage := make([]as3Types.Object, 32)
	for i := range firstPage {
		firstPage[i].Key = aws.String(fmt.Sprintf("repo/refs/tags/%02d", i))
	}
	stub := &listObjectsV2Stub{
		errAt: -1,
		pages: []*as3.ListObjectsV2Output{
			{
				Contents:              firstPage,
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("page-2"),
			},
			{
				Contents:    []as3Types.Object{{Key: aws.String("repo/refs/tags/32")}},
				IsTruncated: aws.Bool(false),
			},
		},
	}
	limit := int32(32)
	objects, err := listS3Objects(context.Background(), stub, &as3.ListObjectsV2Input{
		Bucket:  aws.String("bucket"),
		Prefix:  aws.String("repo/refs/tags/"),
		MaxKeys: &limit,
	})
	if nil != err {
		t.Fatal(err)
	}
	if 33 != len(objects) {
		t.Fatalf("unexpected object count [%d]", len(objects))
	}
	if 2 != stub.calls || 2 != len(stub.tokens) || "" != stub.tokens[0] || "page-2" != stub.tokens[1] {
		t.Fatalf("unexpected pagination calls [%d] and tokens [%v]", stub.calls, stub.tokens)
	}
}

func TestListS3ObjectsReturnsPaginationError(t *testing.T) {
	wantErr := errors.New("list page failed")
	stub := &listObjectsV2Stub{
		errAt:   1,
		pageErr: wantErr,
		pages: []*as3.ListObjectsV2Output{
			{
				Contents:              []as3Types.Object{{Key: aws.String("repo/refs/tags/00")}},
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("page-2"),
			},
		},
	}
	_, err := listS3Objects(context.Background(), stub, &as3.ListObjectsV2Input{
		Bucket: aws.String("bucket"),
		Prefix: aws.String("repo/refs/tags/"),
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected pagination error [%v]", err)
	}
}
