package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	//for printing groutine number
	// "bytes"
	// "runtime"
	// "strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func getClient(bn, cred string) *storage.Client {
	ctx := context.Background()
	var client *storage.Client
	var err error
	if cred != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(cred))
	} else {
		client, err = storage.NewClient(ctx)
	}
	if err != nil {
		fmt.Println(err)
		panic("err: failed to create gcs client")
	}

	bh := client.Bucket(bn)
	if _, err = bh.Attrs(ctx); err != nil {
		fmt.Println(err)
		panic("bucket not found")
	}
	//fmt.Println("client: ", client)
	return client
}

func WalkDir(dt, root string, c chan<- string) error {
	// for tdime small files we generate the paths
	//TODO make dt as function
	if dt == "tdime" {
		for i := 0; i <= 255; i++ {
			for j := 0; j <= 4095; j++ {
				a := fmt.Sprintf("/%02s/%03s", strconv.FormatInt(int64(i), 16), strconv.FormatInt(int64(j), 16))
				c <- a
			}
		}
		close(c)
		return nil
	} else {
		var paths []string
		err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
				return err
			}
			if !info.IsDir() {
				sn := strings.TrimPrefix(path, root)
				fmt.Printf("visited file or dir: %q, %q\n", sn, path)

				paths = append(paths, sn)
			}
			return nil

		})
		if err != nil {
			fmt.Printf("error walking the path %q: %v\n", root, err)
		}

		for _, v := range paths {
			c <- v
		}
		return nil
	}
}

func WalkBucket(dt string, client *storage.Client, path string, c chan<- string) error {
	// for tdime small files we generate the paths
	if dt == "tdime" {
		for i := 0; i <= 255; i++ {
			for j := 0; j <= 4095; j++ {
				a := fmt.Sprintf("/tdime/%02s/%03s", strconv.FormatInt(int64(i), 16), strconv.FormatInt(int64(j), 16))
				c <- a
			}
		}
		close(c)
		return nil
		// for the rest of cases (not tdime)
	} else {
		var paths []string
		m1 := regexp.MustCompile("^gs://([^/]*)/(.*)$")
		bucket := m1.ReplaceAllString(path, "$1")
		prefix := m1.ReplaceAllString(path, "$2")

		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, time.Second*100000)
		defer cancel()

		it := client.Bucket(bucket).Objects(ctx, &storage.Query{
			Prefix: prefix,
		})

		for i := 0; ; i++ {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				fmt.Printf("Bucket(%q).Objects(): %v", bucket, err)
			}
			if !strings.HasSuffix(attrs.Name, "/") {
				// first we put the paths in slice , putting stright to
				// channel doesn't work good
				fmt.Println("now walking: ", i, " ", strings.TrimPrefix(attrs.Name, prefix+"/"))
				paths = append(paths, strings.TrimPrefix(attrs.Name, prefix))
			}

		}
		for _, v := range paths {
			c <- v
		}
		close(c)
		return nil
	}
}

func getBucketName(path string) string {
	m1 := regexp.MustCompile("^gs://([^/]*)/(.*)$")
	bucket := m1.ReplaceAllString(path, "$1")
	return bucket
}

func download(conc int, srcPath, dstPath, cred string, paths <-chan string) error {
	var wg sync.WaitGroup
	wg.Add(conc)

	for i := 0; i < conc; i++ {
		mb := regexp.MustCompile("gs://([^/]*).*")
		bucket := mb.ReplaceAllString(srcPath, "$1")
		fmt.Println("in Download will get client number: ", i)
		client := getClient(bucket, cred)
		defer client.Close()
		bh := client.Bucket(bucket)

		fmt.Println("starting: ", i, "of", conc)
		go func(bh *storage.BucketHandle, fullSrcPath, dstPath string, paths <-chan string) {

			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*500000)
			defer cancel()

			for v := range paths {

				mp := regexp.MustCompile("gs://[^/]*/(.*)")
				obj := mp.ReplaceAllString(fullSrcPath+v, "$1")
				fmt.Println("obj: ", obj)

				md := regexp.MustCompile("(.*)/.*")
				toMkdir := md.ReplaceAllString(dstPath+v, "$1")
				fmt.Println("dstPath: ", dstPath, "   v: ", v)
				//fmt.Printf("%d will download: %q  to %q\n", a, fullSrcPath+v, toMkdir)

				err := os.MkdirAll(toMkdir, os.ModePerm)
				if err != nil {
					fmt.Println(err)
				}

				reader, err := bh.Object(obj).NewReader(ctx)
				if err != nil {
					fmt.Println(err)
				}

				f, err := os.OpenFile(dstPath+v, os.O_CREATE|os.O_WRONLY, os.ModePerm)
				if err != nil {
					f.Close()
					fmt.Println(err)
				}

				_, err = io.Copy(f, reader)
				if err != nil {
					fmt.Println(err)
					panic(err)
				}
				f.Close()
				reader.Close()

			}
			wg.Done()
		}(bh, srcPath, dstPath, paths)
	}
	wg.Wait()

	return nil
}

func upload(conc int, bucket, srcPath, dstPath, cred string, paths <-chan string) error {
	var wg sync.WaitGroup
	wg.Add(conc)

	// remove bucket name from dst path
	m1 := regexp.MustCompile("gs://[^/]*/(.*)")
	dstPath = m1.ReplaceAllString(dstPath, "$1")

	for i := 0; i < conc; i++ {

		client := getClient(bucket, cred)
		defer client.Close()
		bh := client.Bucket(bucket)

		go func(bh *storage.BucketHandle, fullSrcPath, dstPath string, paths <-chan string) {

			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, time.Second*500000)
			defer cancel()

			for v := range paths {

				obj := dstPath + v

				writer := bh.Object(obj).NewWriter(ctx)

				f, err := os.Open(srcPath + v)
				if err != nil {
					f.Close()
					fmt.Println(err)
				}

				_, err = io.Copy(writer, f)
				if err != nil {
					fmt.Println(err)
					panic(err)
				}
				f.Close()
				writer.Close()
				f.Close()

			}
			wg.Done()
		}(bh, srcPath, dstPath, paths)
	}
	wg.Wait()

	return nil
}

func main() {
	paths := make(chan string)
	var err error
	var (
		fcred = flag.String("cred", "", "credential path")
		fin   = flag.String("in", "", "input dir path, starting with gs:// for bucket or just / for dir")
		fout  = flag.String("out", "", "output dir path, starting with gs:// for bucket or just / for dir")
		fconc = flag.Int("conc", 2, "upload cuncurrency")
		fdt   = flag.String("data", "", "tdime in case of tdime")
	)

	flag.Parse()
	cred := *fcred
	in := strings.TrimSuffix(*fin, "/")
	out := strings.TrimSuffix(*fout, "/")
	conc := *fconc
	dt := *fdt

	if in == "" || out == "" {
		fmt.Printf("\nplease provide both 'in' and 'out' parameters\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
	fmt.Printf("---------------------------------------------\n")
	fmt.Printf("credential: %s\t\t\n", cred)
	fmt.Printf("input:      %s\t\t\n", in)
	fmt.Printf("output:     %s\t\t\n", out)
	fmt.Printf("concurrent workers:     %d\t\t\n", conc)
	fmt.Printf("---------------------------------------------\n\n")

	if strings.HasPrefix(in, "gs://") {
		getBucketName(in)
		go WalkBucket(dt, getClient(getBucketName(in), cred), in, paths)
		if download(conc, in, out, cred, paths); err != nil {
			fmt.Println(err)
		}

	} else if strings.HasPrefix(out, "gs://") {
		go WalkDir(dt, in, paths)
		if upload(conc, getBucketName(out), in, out, cred, paths); err != nil {
			fmt.Println(err)
		}

	}
}
