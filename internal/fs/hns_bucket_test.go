// Copyright 2024 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fs_test

import (
	"os"
	"path"
	"testing"

	"github.com/googlecloudplatform/gcsfuse/v2/cfg"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/config"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/storage/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type HNSBucketTests struct {
	suite.Suite
	fsTest
}

func TestHNSBucketTests(t *testing.T) { suite.Run(t, new(HNSBucketTests)) }

func (t *HNSBucketTests) SetupSuite() {
	t.serverCfg.ImplicitDirectories = false
	bucketType = gcs.Hierarchical
	t.serverCfg.MountConfig = &config.MountConfig{
		WriteConfig: cfg.WriteConfig{
			CreateEmptyFile: false,
		},
		EnableHNS: true,
	}
	t.fsTest.SetUpTestSuite()
}

func (t *HNSBucketTests) TearDownSuite() {
	t.fsTest.TearDownTestSuite()
}

func (t *HNSBucketTests) SetupTest() {
	err = t.createFolders([]string{"foo/", "bar/", "foo/test2/", "foo/test/"})
	require.NoError(t.T(), err)

	err = t.createObjects(
		map[string]string{
			"foo/file1.txt":              "abcdef",
			"foo/file2.txt":              "xyz",
			"foo/test/file3.txt":         "xyz",
			"foo/implicit_dir/file3.txt": "xxw",
			"bar/file1.txt":              "-1234556789",
		})
	require.NoError(t.T(), err)
}

func (t *HNSBucketTests) TearDown() {
	t.fsTest.TearDown()
}

func (t *HNSBucketTests) TestReadDir() {
	dirPath := path.Join(mntDir, "foo")

	dirEntries, err := os.ReadDir(dirPath)

	assert.NoError(t.T(), err)
	assert.Equal(t.T(), 5, len(dirEntries))
	for i := 0; i < 5; i++ {
		switch dirEntries[i].Name() {
		case "test":
			assert.Equal(t.T(), "test", dirEntries[i].Name())
			assert.True(t.T(), dirEntries[i].IsDir())
		case "test2":
			assert.Equal(t.T(), "test2", dirEntries[i].Name())
			assert.True(t.T(), dirEntries[i].IsDir())
		case "file1.txt":
			assert.Equal(t.T(), "file1.txt", dirEntries[i].Name())
			assert.False(t.T(), dirEntries[i].IsDir())
		case "file2.txt":
			assert.Equal(t.T(), "file2.txt", dirEntries[i].Name())
			assert.False(t.T(), dirEntries[i].IsDir())
		case "implicit_dir":
			assert.Equal(t.T(), "implicit_dir", dirEntries[i].Name())
			assert.True(t.T(), dirEntries[i].IsDir())
		}
	}
}

func (t *HNSBucketTests) TestDeleteFolder() {
	dirPath := path.Join(mntDir, "foo")

	err = os.RemoveAll(dirPath)

	assert.NoError(t.T(), err)
	_, err = os.Stat(dirPath)
	assert.NotNil(t.T(), err)
}

func (t *HNSBucketTests)  TestCreateLocalFileInSamePathAfterDeletingParentDirectory() {
	dirPath := path.Join(mntDir, "foo", "test2")
	filePath := path.Join(dirPath, "test.txt")
	f1, err := os.Create(filePath)
	defer require.NoError(t.T(),f1.Close())
	require.NoError(t.T(), err)
	_, err = os.Stat(filePath)
	require.NoError(t.T(), err)
	err = os.RemoveAll(dirPath)
	assert.NoError(t.T(), err)
	err = os.Mkdir(dirPath, 0755)
	assert.NoError(t.T(), err)

	f2, err := os.Create(filePath)
	defer require.NoError(t.T(), f2.Close())

	assert.NoError(t.T(), err)
}