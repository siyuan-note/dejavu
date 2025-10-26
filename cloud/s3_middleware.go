package cloud

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// HeadersToIgnore lists headers that frequently cause SignatureDoesNotMatch errors
// when used with S3-compatible providers behind proxies (like Cloudflare Tunnel or GCS).
// These headers are temporarily removed before the SigV4 signing process and restored afterwards.
var HeadersToIgnore = []string{
	"Accept-Encoding", // The primary culprit, often modified by proxies.
	"Amz-Sdk-Invocation-Id",
	"Amz-Sdk-Request",
}

type ignoredHeadersKey struct{}

// IgnoreSigningHeaders is a helper to inject middleware that excludes specified headers
// from the Signature Version 4 calculation by temporarily removing them.
// This function should be called only for non-AWS S3 endpoints.
func IgnoreSigningHeaders(o *s3.Options, headers []string) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		// 1. Insert ignoreHeaders BEFORE the "Signing" middleware
		if err := stack.Finalize.Insert(ignoreHeaders(headers), "Signing", middleware.Before); err != nil {
			return fmt.Errorf("failed to insert S3CompatIgnoreHeaders: %w", err)
		}

		// 2. Insert restoreIgnored AFTER the "Signing" middleware
		if err := stack.Finalize.Insert(restoreIgnored(), "Signing", middleware.After); err != nil {
			return fmt.Errorf("failed to insert S3CompatRestoreHeaders: %w", err)
		}

		return nil
	})
}

// ignoreHeaders removes specified headers and stores them in context for later restoration.
func ignoreHeaders(headers []string) middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"S3CompatIgnoreHeaders",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
			req, ok := in.Request.(*smithyhttp.Request)
			if !ok {
				return out, metadata, &v4.SigningError{Err: errors.New("unexpected request middleware type for ignoreHeaders")}
			}

			// Store removed headers and their values
			ignored := make(map[string]string, len(headers))
			for _, h := range headers {
				// Use canonical form for map key (e.g., "Accept-Encoding")
				// strings.Title is necessary for older Go versions to ensure canonicalization.
				canonicalKey := strings.Title(strings.ToLower(h))
				ignored[canonicalKey] = req.Header.Get(h)
				req.Header.Del(h) // Remove header before signing
			}

			// Store the ignored headers in the context
			ctx = middleware.WithStackValue(ctx, ignoredHeadersKey{}, ignored)

			return next.HandleFinalize(ctx, in)
		},
	)
}

// restoreIgnored retrieves headers from context and restores them to the request
// after the signing (Finalize) and before sending.
func restoreIgnored() middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"S3CompatRestoreHeaders",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
			// Execute the next handler first (which includes the signing)
			out, metadata, err = next.HandleFinalize(ctx, in)

			req, ok := in.Request.(*smithyhttp.Request)
			if !ok {
				return out, metadata, errors.New("unexpected request middleware type for restoreIgnored")
			}

			// Retrieve ignored headers from the context
			ignored, _ := middleware.GetStackValue(ctx, ignoredHeadersKey{}).(map[string]string)
			// Restore the headers to the request
			for k, v := range ignored {
				if v != "" {
					req.Header.Set(k, v)
				}
			}

			return out, metadata, err
		},
	)
}
