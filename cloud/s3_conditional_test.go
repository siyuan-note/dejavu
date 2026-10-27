package cloud

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type conditionalTransport func(*http.Request) (*http.Response, error)

func (transport conditionalTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestS3ConditionalDownload(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          int
		connectionError bool
		wantCalls       int
		wantUnchanged   bool
		wantError       bool
	}{
		{"not-modified", 304, false, 1, true, false},
		{"unsupported", 501, false, 2, false, false},
		{"connection-error", 0, true, 2, false, false},
		{"forbidden", 403, false, 1, false, true},
		{"unauthorized", 401, false, 1, false, true},
		{"missing", 404, false, 1, false, true},
		{"ignored", 200, false, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			etag := `W/"opaque-multipart-2"`
			client := &http.Client{Transport: conditionalTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				status := http.StatusOK
				if calls == 1 {
					if r.Header.Get("If-None-Match") != etag {
						t.Fatalf("ETag changed: %q", r.Header.Get("If-None-Match"))
					}
					if tc.connectionError {
						return nil, errors.New("connection closed")
					}
					status = tc.status
				} else if r.Header.Get("If-None-Match") != "" {
					t.Fatal("fallback retained conditional header")
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Etag": {etag}},
					Body: io.NopCloser(strings.NewReader("body")), Request: r}, nil
			})}
			s3 := NewS3(&BaseCloud{Conf: &Conf{S3: &ConfS3{Bucket: "bucket", Timeout: 5}}}, client)
			s3.service = as3.New(as3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://localhost"),
				UsePathStyle: true, Credentials: aws.AnonymousCredentials{}, HTTPClient: client, RetryMaxAttempts: 1})
			data, gotETag, unchanged, err := s3.DownloadObjectIfChanged("indexes-v2.json", etag)
			if (err != nil) != tc.wantError || unchanged != tc.wantUnchanged || calls != tc.wantCalls {
				t.Fatalf("calls=%d unchanged=%v err=%v", calls, unchanged, err)
			}
			if !tc.wantError && gotETag != etag {
				t.Fatalf("response ETag changed: %q", gotETag)
			}
			if unchanged && len(data) != 0 {
				t.Fatal("304 returned a body")
			}
			if tc.status == 404 && !errors.Is(err, ErrCloudObjectNotFound) {
				t.Fatalf("missing error: %v", err)
			}
		})
	}
}
