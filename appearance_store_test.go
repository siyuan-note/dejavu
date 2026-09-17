package dejavu

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func TestAppearanceSharedChunkConcurrentWriteAndRead(t *testing.T) {
	repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "shared", false)
	for round := range 8 {
		data := make([]byte, 2<<20)
		copy(data, fmt.Sprintf("shared chunk round %d", round))
		chunk := &entity.Chunk{ID: util.Hash(data), Data: data}
		start := make(chan struct{})
		errs := make(chan error, 12)
		var done sync.WaitGroup
		for range 12 {
			done.Add(1)
			go func() {
				defer done.Done()
				<-start
				if err := repo.store.PutChunk(chunk); err != nil {
					errs <- err
					return
				}
				read, err := repo.store.GetChunk(chunk.ID)
				if err != nil {
					errs <- err
				} else if !bytes.Equal(read.Data, data) {
					errs <- fmt.Errorf("shared chunk plaintext changed")
				}
			}()
		}
		close(start)
		done.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
}
