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

package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
	"sync"
)

var globalLocalizer *i18n.Localizer

func getLocalizer() (*i18n.Localizer, error) {
	mutex := &sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()

	if globalLocalizer != nil {
		return globalLocalizer, nil
	}

	bundle := i18n.NewBundle(language.English)
	bundle.RegisterUnmarshalFunc("json", json.Unmarshal)
	_, err := bundle.LoadMessageFile("../resources/en.json")
	if err != nil {
		return nil, errors.New("could not load messages file")
	}
	globalLocalizer = i18n.NewLocalizer(bundle, language.English.String())
	return globalLocalizer, nil
}

func GetMessage(messageId string, messageArgs ...interface{}) string {
	localizer, err := getLocalizer()
	if err != nil {
		panic(err)
	}

	localizeConfigWelcome := i18n.LocalizeConfig{
		MessageID: messageId,
	}
	localizationUsingJson, _ := localizer.Localize(&localizeConfigWelcome)
	return fmt.Sprintf(localizationUsingJson, messageArgs...)
}
