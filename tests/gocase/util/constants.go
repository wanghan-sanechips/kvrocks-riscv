/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package util

import "time"

const DefaultDelta = 0.000001

// Kubernetes will send a SIGTERM signal to the containers in the pod after deleting the pod.
// It waits for a specified time, called the termination grace period. By default, this is 30 seconds.
// If the containers are still running after the grace period,
// they are sent the SIGKILL signal and forcibly removed.
const defaultGracePeriod = 30 * time.Second
