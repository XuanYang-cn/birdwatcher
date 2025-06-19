package states

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type ReadDescriptorParams struct {
	framework.ParamBase `use:"read-desc" desc:"desc"`
	SegmentID           int64  `name:"segment" default:"0"`
	MinioAddress        string `name:"minioAddr"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check due to permission issue"`
	WorkerNum           int64  `name:"workerNum" default:"4" desc:"worker num"`
}

func (s *InstanceState) ReadDescriptorCommand(ctx context.Context, p *ReadDescriptorParams) error {
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return (p.SegmentID == 0 || p.SegmentID == s.ID)
	})
	if err != nil {
		return err
	}

	params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		fmt.Println("Failed to create client,", err.Error())
		return err
	}

	fmt.Printf("=== start to execute ===\n")
	fmt.Printf("=== worker num: %d ===\n", p.WorkerNum)

	getObject := func(binlogPath string) (*minio.Object, error) {
		logPath := strings.ReplaceAll(binlogPath, "ROOT_PATH", rootPath)
		return minioClient.GetObject(ctx, bucketName, logPath, minio.GetObjectOptions{})
	}

	normalSegments := lo.Filter(segments, func(segment *models.Segment, _ int) bool {
		return segment.Level != datapb.SegmentLevel_L0
	})

	workFn := func(segment *models.Segment) error {
		rowIDFields := segment.GetBinlogs()[0]
		fmt.Printf("--- SegmentID=%d, field=%v, descriptors=\n", segment.ID, rowIDFields.FieldID)
		for _, binlog := range rowIDFields.Binlogs {
			rowIDObj, err := getObject(binlog.LogPath)
			if err != nil {
				return err
			}
			_, desc, err := storage.NewBinlogReader(rowIDObj)
			fmt.Printf("--- -- des, descriptors=%v\n", desc.Extras)
		}
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(int(p.WorkerNum))
	taskCh := make(chan *models.Segment)

	num := atomic.NewInt64(0)

	for i := 0; i < int(p.WorkerNum); i++ {
		go func() {
			defer wg.Done()
			for {
				segment, ok := <-taskCh
				if !ok {
					return
				}
				err := workFn(segment)
				if err != nil {
					fmt.Println(err)
				}
				fmt.Printf("%d/%d done\n", num.Inc(), len(normalSegments))
			}
		}()
	}

	for _, segment := range normalSegments {
		taskCh <- segment
	}
	close(taskCh)
	wg.Wait()

	return nil
}
