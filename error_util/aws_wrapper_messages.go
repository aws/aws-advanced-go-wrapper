/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package error_util

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

var globalLocalizer *i18n.Localizer
var LocalizerMutex = &sync.Mutex{}

func getLocalizer() (*i18n.Localizer, error) {
	LocalizerMutex.Lock()
	defer LocalizerMutex.Unlock()

	if globalLocalizer != nil {
		return globalLocalizer, nil
	}

	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	path := filepath.Join(dir, "../resources/en.json")
	bundle := i18n.NewBundle(language.English)
	bundle.RegisterUnmarshalFunc("json", json.Unmarshal)

	_, err := bundle.LoadMessageFile(path)
	if err != nil {
		return nil, errors.New("could not load messages file")
	}

	globalLocalizer = i18n.NewLocalizer(bundle, language.English.String())
	return globalLocalizer, nil
}

func GetMessage(messageId string, messageArgs ...any) string {
	localizer, err := getLocalizer()
	if err != nil {
		return fmt.Sprintf("Unable to display message %s with arguments %v. Error: %s.", messageId, messageArgs, err.Error())
	}

	localizeConfigWelcome := i18n.LocalizeConfig{
		MessageID: messageId,
	}
	localizationUsingJson, _ := localizer.Localize(&localizeConfigWelcome)
	return fmt.Sprintf(localizationUsingJson, messageArgs...)
}
