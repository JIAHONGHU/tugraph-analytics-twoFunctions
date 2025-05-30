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

 package com.antgroup.geaflow.dsl.udf.graph;

 import com.antgroup.geaflow.common.tuple.Tuple;
 import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
 import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
 import com.antgroup.geaflow.dsl.common.data.Row;
 import com.antgroup.geaflow.dsl.common.data.RowEdge;
 import com.antgroup.geaflow.dsl.common.data.RowVertex;
 import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
 import com.antgroup.geaflow.dsl.common.function.Description;
 import com.antgroup.geaflow.dsl.common.types.GraphSchema;
 import com.antgroup.geaflow.dsl.common.types.StructType;
 import com.antgroup.geaflow.dsl.common.types.TableField;
 import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
 import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.HashSet;
import java.util.Iterator;
 import java.util.List;
 import java.util.Optional;



 @Description(name = "common_neighbors_for_vertex_set", description = "built-in udga for CommonNeighborsForVertexSet")
public class CommonNeighborsForVertexSet implements AlgorithmUserFunction<Object, Tuple<Boolean, Boolean>> {

    private AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context;


	HashSet<Object> A = new HashSet<>();
	HashSet<Object> B = new HashSet<>();

    @Override
    public void init(AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context, Object[] params) {
        this.context = context;
		Boolean sep = false;
		int idx = 0;
		Class<?> idClazz = context.getGraphSchema().getIdType().getTypeClass();
		for (Object param : params) {
			if (param instanceof String && ((String) param).equals("|")) {
				sep = true;
				idx ++;
				break;
			} else if (idClazz.isInstance(param)) {
				A.add(TypeCastUtil.cast(param, context.getGraphSchema().getIdType()));
				idx ++;
			}
			else{
				throw new IllegalArgumentException("Only support string and id type argument, usage: common_neighbors_for_vertex_set(id_a, id_b, |, id_c, id_d)");
			}
		}
		if (!sep) {
			throw new IllegalArgumentException("Only support string and id type argument, usage: common_neighbors_for_vertex_set(id_a, id_b, |, id_c, id_d)");
		}
		for (int i = idx; i < params.length; i++) {
			B.add(TypeCastUtil.cast(params[i], context.getGraphSchema().getIdType()));
		}
		if (B.size() == 0){
			throw new IllegalArgumentException("Only support string and id type argument, usage: common_neighbors_for_vertex_set(id_a, id_b, |, id_c, id_d)");
		}
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Boolean, Boolean>> messages) {
        if (context.getCurrentIterationId() == 1L) {
            // send message to neighbors if they are vertices in params
			Tuple<Boolean, Boolean> f = new Tuple<>(false, false);
            if (A.contains(vertex.getId())) {
				f.setF0(true);
			}
			if (B.contains(vertex.getId())) {
				f.setF1(true);
			}
            sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), f);
        } else if (context.getCurrentIterationId() == 2L) {
            // add to result if received messages from both vertices in params
            Tuple<Boolean, Boolean> received = new Tuple<>(false, false);
            while (messages.hasNext()) {
                Tuple<Boolean, Boolean> message = messages.next();
				if(A.contains(vertex.getId()) || B.contains(vertex.getId()))
				continue;
                received.setF0(received.getF0() || message.getF0());
                received.setF1(received.getF1() || message.getF1());
            }
			if (received.getF0() && received.getF1()) {
				context.take(ObjectRow.create(vertex.getId()));
			}
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("id", graphSchema.getIdType(), false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Tuple<Boolean, Boolean> message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}