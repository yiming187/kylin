/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.Properties;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/config", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NConfigController extends NBasicController {

    @ApiOperation(value = "is cloud", tags = { "MID" })
    @GetMapping(value = "/is_cloud")
    @ResponseBody
    public EnvelopeResponse<Boolean> isCloud() {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, !(kapConfig.getChannelUser().equals("on-premises")),
                "");
    }

    @ApiOperation(value = "fetch all", tags = { "MID" })
    @GetMapping(value = "/all")
    @ResponseBody
    public EnvelopeResponse<Properties> fetchAll() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                KylinConfig.getInstanceFromEnv().exportToProperties(), "");
    }
}
