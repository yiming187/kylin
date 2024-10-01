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

package org.apache.kylin.rec;

import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;

public class ModelInfoAdjustProposer extends AbstractProposer {

    public ModelInfoAdjustProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        for (AbstractContext.ModelContext modelCtx : proposeContext.getModelContexts()) {
            if (modelCtx.isTargetModelMissing()) {
                continue;
            }

            NDataModel model = modelCtx.getTargetModel();
            modelCtx.getProposeContext().changeModelMainType(model);
            setJoinTableType(model);
            model.setModelType(model.getModelTypeFromTable());
        }
    }

    private void setJoinTableType(NDataModel model) {
        for (JoinTableDesc joinTableDesc : model.getJoinTables()) {
            if (!joinTableDesc.getTableRef().equals(model.getRootFactTableRef())) {
                joinTableDesc.setKind(NDataModel.TableKind.LOOKUP);
            }
        }
    }

    @Override
    public String getIdentifierName() {
        return "ModelInfoAdjustProposer";
    }
}