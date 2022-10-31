package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3Session *s3.S3
)

const (
	BUCKET_NAME = "go-test-s3"
	REGION      = "eu-central-1"
)

func init() {
	s3Session = s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(REGION),
	})))
}

func listBuckets() (resp *s3.ListBucketsOutput) {
	resp, err := s3Session.ListBuckets(&s3.ListBucketsInput{})

	if err != nil {
		panic(err)
	}

	return resp
}

func createBucket() (resp *s3.CreateBucketOutput) {
	resp, err := s3Session.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(BUCKET_NAME),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(REGION),
		},
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println("Bucket name already in use!")
				panic(err)
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				fmt.Println("Bucket exists and is owned by you!")
			default:
				panic(err)
			}
		}
	}
	return resp
}

func uploadObject(filename string) (resp *s3.PutObjectOutput) {
	f, err := os.Open(filename)

	if err != nil {
		panic(err)
	}

	fmt.Println("Uploading: ", filename)

	resp, err = s3Session.PutObject(&s3.PutObjectInput{
		Body:   f,
		Bucket: aws.String(BUCKET_NAME),
		Key:    aws.String(strings.Split(filename, "/")[1]),
		ACL:    aws.String(s3.BucketCannedACLPublicRead),
	})

	if err != nil {
		panic(err)
	}
	return resp
}

func listObjects() (resp *s3.ListObjectsV2Output) {
	resp, err := s3Session.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(BUCKET_NAME),
	})

	if err != nil {
		panic(err)
	}
	return resp
}

func getObject(filename string) {
	fmt.Println("Downloading: ", filename)

	resp, err := s3Session.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET_NAME),
		Key:    aws.String(filename),
	})
	if err != nil {
		panic(err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	err = os.WriteFile(filename, body, 0644)

	if err != nil {
		panic(err)
	}
}

func deleteObject(filename string) (resp *s3.DeleteObjectOutput) {
	fmt.Println("Deleting: ", filename)

	resp, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(BUCKET_NAME),
		Key:    aws.String(filename),
	})

	if err != nil {
		panic(err)
	}

	return resp
}

func main() {
	folder := "files"

	files, _ := os.ReadDir(folder)

	fmt.Println(files)

	for _, file := range files {
		if file.IsDir() {
			continue
		} else {
			uploadObject(folder + "/" + file.Name())
		}
	}

	fmt.Println(listObjects())

	for _, object := range listObjects().Contents {
		getObject(*object.Key)
		deleteObject(*object.Key)
	}

	fmt.Println(listObjects())
}
