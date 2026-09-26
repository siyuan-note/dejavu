package dejavu

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
)

type failingLockCloud struct {
	cloud.Cloud
	err error
}

func (c *failingLockCloud) UploadObject(string, bool) (int64, error) {
	return 0, c.err
}

func TestCloudLockUploadErrorCause(t *testing.T) {
	dnsErr := &net.DNSError{Err: "no such host", Name: "up.qbox.me", IsNotFound: true}
	for _, cause := range []error{
		&url.Error{Op: "Post", URL: "https://up.qbox.me/?token=secret", Err: dnsErr},
		&net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 503}, Err: errors.New("connection refused")},
		context.DeadlineExceeded,
		errors.New("unknown upload failure"),
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			repo := &Repo{Path: t.TempDir(), cloud: &failingLockCloud{err: cause}}
			err := repo.lockCloud0("self")
			if !errors.Is(err, ErrLockCloudFailed) || !errors.Is(err, cause) || errors.Is(err, ErrCloudLocked) {
				t.Fatalf("lost lock failure or original cause: %v", err)
			}
			if err.Error() != ErrLockCloudFailed.Error() {
				t.Fatalf("default message exposed the underlying request: %v", err)
			}
			if errors.Is(cause, dnsErr) {
				var actual *net.DNSError
				if !errors.As(err, &actual) || actual != dnsErr {
					t.Fatalf("lost DNS error type: %v", err)
				}
			}
		})
	}
}

func TestCloudLockUploadKnownErrors(t *testing.T) {
	for _, tc := range []struct{ cause, want error }{
		{cloud.ErrSystemTimeIncorrect, cloud.ErrSystemTimeIncorrect},
		{cloud.ErrCloudAuthFailed, cloud.ErrCloudAuthFailed},
		{cloud.ErrDeprecatedVersion, cloud.ErrDeprecatedVersion},
		{cloud.ErrCloudCheckFailed, cloud.ErrCloudCheckFailed},
		{&url.Error{URL: "https://example.com", Err: cloud.ErrCloudForbidden}, cloud.ErrCloudForbidden},
		{&url.Error{URL: "https://example.com", Err: cloud.ErrCloudServiceUnavailable}, cloud.ErrCloudServiceUnavailable},
		{&url.Error{URL: "https://example.com", Err: cloud.ErrCloudTooManyRequests}, cloud.ErrCloudTooManyRequests},
		{errors.New("HTTP 403 Forbidden"), cloud.ErrCloudForbidden},
		{errors.New("HTTP 429 Too Many Requests"), cloud.ErrCloudTooManyRequests},
		{errors.New("HTTP 503 Service Unavailable"), cloud.ErrCloudServiceUnavailable},
		{errors.New("RequestTimeTooSkewed"), cloud.ErrSystemTimeIncorrect},
	} {
		t.Run(tc.cause.Error(), func(t *testing.T) {
			repo := &Repo{Path: t.TempDir(), cloud: &failingLockCloud{err: tc.cause}}
			if err := repo.lockCloud0("self"); !errors.Is(err, tc.want) || errors.Is(err, ErrCloudLocked) {
				t.Fatalf("unexpected classified error: %v", err)
			}
		})
	}
}

func TestCloudLockLocalWriteFailure(t *testing.T) {
	repo := &Repo{Path: t.TempDir()}
	// 用目录占据锁文件路径，确保所有平台都能稳定触发本地写入失败。
	if err := os.Mkdir(filepath.Join(repo.Path, lockSyncKey), 0755); nil != err {
		t.Fatal(err)
	}
	err := repo.lockCloud0("self")
	if !errors.Is(err, ErrLockCloudFailed) || errors.Is(err, ErrCloudLocked) {
		t.Fatalf("local write failure was classified as another device's lock: %v", err)
	}
	var pathErr *os.PathError
	var linkErr *os.LinkError
	if !errors.As(err, &pathErr) && !errors.As(err, &linkErr) {
		t.Fatalf("lost local write error: %v", err)
	}
}
