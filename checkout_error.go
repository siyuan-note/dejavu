package dejavu

import "fmt"

// CheckoutError 标明恢复失败的阶段，以及失败前是否可能已改写工作空间。
// MayHaveApplied 为 false 时仅写入仓库对象或状态，没有替换或删除工作空间文件。
type CheckoutError struct {
	Stage          string
	MayHaveApplied bool
	Err            error
}

func (err *CheckoutError) Error() string {
	return fmt.Sprintf("checkout %s: %s", err.Stage, err.Err)
}

func (err *CheckoutError) Unwrap() error { return err.Err }
