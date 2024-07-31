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

package cfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRationalizeEnableEmptyManagedFolders(t *testing.T) {
	testcases := []struct {
		name                              string
		enableHns                         bool
		enableEmptyManagedFolders         bool
		expectedEnableEmptyManagedFolders bool
	}{
		{
			name:                              "both enable-hns and enable-empty-managed-folders set to true",
			enableHns:                         true,
			enableEmptyManagedFolders:         true,
			expectedEnableEmptyManagedFolders: true,
		},
		{
			name:                              "enable-hns set to true and enable-empty-managed-folders set to false",
			enableHns:                         true,
			enableEmptyManagedFolders:         false,
			expectedEnableEmptyManagedFolders: true,
		},
		{
			name:                              "enable-hns set to false and enable-empty-managed-folders set to true",
			enableHns:                         false,
			enableEmptyManagedFolders:         true,
			expectedEnableEmptyManagedFolders: true,
		},
		{
			name:                              "both enable-hns and enable-empty-managed-folders set to false",
			enableHns:                         false,
			enableEmptyManagedFolders:         false,
			expectedEnableEmptyManagedFolders: false,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{
				EnableHns: tc.enableHns,
				List:      ListConfig{EnableEmptyManagedFolders: tc.enableEmptyManagedFolders},
			}

			err := Rationalize(&c)

			if assert.NoError(t, err) {
				assert.Equal(t, tc.expectedEnableEmptyManagedFolders, c.List.EnableEmptyManagedFolders)
			}
		})
	}
}

func TestRationalizeCustomEndpointSuccessful(t *testing.T) {
	testCases := []struct {
		name                   string
		config                 *Config
		expectedCustomEndpoint string
	}{
		{
			name: "Valid Config where input and expected custom endpoint matches.",
			config: &Config{
				GcsConnection: GcsConnectionConfig{
					CustomEndpoint: "https://bing.com/search?q=dotnet",
				},
			},
			expectedCustomEndpoint: "https://bing.com/search?q=dotnet",
		},
		{
			name: "Valid Config where input and expected custom endpoint differ.",
			config: &Config{
				GcsConnection: GcsConnectionConfig{
					CustomEndpoint: "https://j@ne:password@google.com",
				},
			},
			expectedCustomEndpoint: "https://j%40ne:password@google.com",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualErr := Rationalize(tc.config)

			assert.NoError(t, actualErr)
			assert.Equal(t, tc.expectedCustomEndpoint, tc.config.GcsConnection.CustomEndpoint)
		})
	}
}

func TestRationalizeCustomEndpointUnsuccessful(t *testing.T) {
	testCases := []struct {
		name   string
		config *Config
	}{
		{
			name: "Invalid Config",
			config: &Config{
				GcsConnection: GcsConnectionConfig{
					CustomEndpoint: "a_b://abc",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualErr := Rationalize(tc.config)

			assert.Error(t, actualErr)
			assert.Equal(t, "", tc.config.GcsConnection.CustomEndpoint)
		})
	}
}
