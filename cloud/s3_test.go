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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
	as3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestS3GetNotFoundConcurrentResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "present") {
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	s3 := NewS3(&BaseCloud{Conf: &Conf{S3: &ConfS3{Endpoint: server.URL, Bucket: "bucket",
		Region: "test", AccessKey: "test", SecretKey: "test", PathStyle: true, Timeout: 5, ConcurrentReqs: 16}}}, server.Client())
	keys := []string{"present"}
	for i := 0; i < 256; i++ {
		keys = append(keys, fmt.Sprintf("missing-%d", i))
	}
	for iteration := 0; iteration < 3; iteration++ {
		missing, err := s3.getNotFound(keys)
		if err != nil {
			t.Fatal(err)
		}
		found := map[string]bool{}
		for _, key := range missing {
			if key == "present" || found[key] {
				t.Fatalf("unexpected missing key: %s", key)
			}
			found[key] = true
		}
		if len(found) != len(keys)-1 {
			t.Fatalf("missing results: got %d, want %d", len(found), len(keys)-1)
		}
		for _, key := range keys[1:] {
			if !found[key] {
				t.Fatalf("lost missing key: %s", key)
			}
		}
	}
}

type listObjectsStub struct {
	pages   []*as3.ListObjectsOutput
	errAt   int
	pageErr error
	markers []string
	calls   int
}

func (stub *listObjectsStub) ListObjects(_ context.Context, input *as3.ListObjectsInput,
	_ ...func(*as3.Options)) (*as3.ListObjectsOutput, error) {
	marker := ""
	if nil != input.Marker {
		marker = *input.Marker
	}
	stub.markers = append(stub.markers, marker)
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
	stub := &listObjectsStub{
		errAt: -1,
		pages: []*as3.ListObjectsOutput{
			{
				Contents:    firstPage,
				IsTruncated: aws.Bool(true),
				NextMarker:  aws.String("repo/refs/tags/31"),
			},
			{
				Contents:    []as3Types.Object{{Key: aws.String("repo/refs/tags/32")}},
				IsTruncated: aws.Bool(false),
			},
		},
	}
	limit := int32(32)
	objects, err := listS3Objects(context.Background(), stub, &as3.ListObjectsInput{
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
	if 2 != stub.calls || 2 != len(stub.markers) || "" != stub.markers[0] || "repo/refs/tags/31" != stub.markers[1] {
		t.Fatalf("unexpected pagination calls [%d] and markers [%v]", stub.calls, stub.markers)
	}
}

func TestListS3ObjectsReturnsPaginationError(t *testing.T) {
	wantErr := errors.New("list page failed")
	stub := &listObjectsStub{
		errAt:   1,
		pageErr: wantErr,
		pages: []*as3.ListObjectsOutput{
			{
				Contents:    []as3Types.Object{{Key: aws.String("repo/refs/tags/00")}},
				IsTruncated: aws.Bool(true),
				NextMarker:  aws.String("repo/refs/tags/00"),
			},
		},
	}
	_, err := listS3Objects(context.Background(), stub, &as3.ListObjectsInput{
		Bucket: aws.String("bucket"),
		Prefix: aws.String("repo/refs/tags/"),
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected pagination error [%v]", err)
	}
}

func TestListS3ObjectsUsesLastKeyAsMarker(t *testing.T) {
	stub := &listObjectsStub{
		errAt: -1,
		pages: []*as3.ListObjectsOutput{
			{
				Contents:    []as3Types.Object{{Key: aws.String("repo/refs/tags/00")}},
				IsTruncated: aws.Bool(true),
			},
			{
				Contents:    []as3Types.Object{{Key: aws.String("repo/refs/tags/01")}},
				IsTruncated: aws.Bool(false),
			},
		},
	}
	objects, err := listS3Objects(context.Background(), stub, &as3.ListObjectsInput{
		Bucket: aws.String("bucket"),
		Prefix: aws.String("repo/refs/tags/"),
	})
	if nil != err {
		t.Fatal(err)
	}
	if 2 != len(objects) || 2 != stub.calls || "repo/refs/tags/00" != stub.markers[1] {
		t.Fatalf("unexpected objects [%d], calls [%d], and markers [%v]", len(objects), stub.calls, stub.markers)
	}
}

func TestListS3ObjectsRejectsUnchangedMarker(t *testing.T) {
	stub := &listObjectsStub{
		errAt: -1,
		pages: []*as3.ListObjectsOutput{
			{
				IsTruncated: aws.Bool(true),
				NextMarker:  aws.String("marker"),
			},
		},
	}
	_, err := listS3Objects(context.Background(), stub, &as3.ListObjectsInput{
		Bucket: aws.String("bucket"),
		Marker: aws.String("marker"),
	})
	if nil == err || "list objects marker did not advance" != err.Error() {
		t.Fatalf("unexpected marker error [%v]", err)
	}
}

func TestListS3ObjectsReturnsEmptyList(t *testing.T) {
	stub := &listObjectsStub{
		errAt: -1,
		pages: []*as3.ListObjectsOutput{
			{IsTruncated: aws.Bool(false)},
		},
	}
	objects, err := listS3Objects(context.Background(), stub, &as3.ListObjectsInput{
		Bucket: aws.String("bucket"),
		Prefix: aws.String("repo/refs/tags/"),
	})
	if nil != err {
		t.Fatal(err)
	}
	if nil == objects || 0 != len(objects) {
		t.Fatalf("unexpected objects [%v]", objects)
	}
}
