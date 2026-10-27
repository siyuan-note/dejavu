package cloud

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// DownloadObjectIfChanged 原样回传 ETag，304 返回 notModified，不把 ETag 当作内容校验和。
// 条件请求不兼容时去除条件头重试一次；鉴权、对象不存在和取消错误直接返回。
func (s3 *S3) DownloadObjectIfChanged(filePath, etag string) (data []byte, currentETag string, notModified bool, err error) {
	ctx, cancel := context.WithTimeout(s3.Context(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancel()
	input := &as3.GetObjectInput{
		Bucket: aws.String(s3.Conf.S3.Bucket), Key: aws.String(path.Join("repo", filePath)),
		ResponseCacheControl: aws.String("no-cache"),
	}
	if etag != "" {
		input.IfNoneMatch = aws.String(etag)
	}
	response, err := s3.getService().GetObject(ctx, input)
	if err != nil && etag != "" {
		var responseErr *smithyhttp.ResponseError
		if errors.As(err, &responseErr) {
			switch responseErr.HTTPStatusCode() {
			case http.StatusNotModified:
				return nil, etag, true, nil
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
				return nil, "", false, s3.snapshotDownloadError(err)
			}
		}
		if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			input.IfNoneMatch = nil
			response, err = s3.getService().GetObject(ctx, input)
		}
	}
	if err != nil {
		return nil, "", false, s3.snapshotDownloadError(err)
	}
	defer response.Body.Close()
	data, err = io.ReadAll(response.Body)
	return data, aws.ToString(response.ETag), false, err
}

func (s3 *S3) snapshotDownloadError(err error) error {
	if s3.isErrNotFound(err) {
		return ErrCloudObjectNotFound
	}
	return err
}
