/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.HashSet; // 用于跟踪已访问节点以避免无限循环和重复消息

/**
 * 单点环路检测算法。
 * 该算法检测从指定起始顶点出发，并最终返回到该起始顶点的环路。
 * 使用消息传递模拟图遍历，消息携带 (起点ID, 当前路径长度)。
 */
@Description(name = "single_node_cycle_detection", description = "built-in udga for single node cycle detection")
public class SingleNodeCycleDetection implements AlgorithmUserFunction<Object, Tuple<Object, Integer>> {

    private AlgorithmRuntimeContext<Object, Tuple<Object, Integer>> context;
    private Object startVertexId; // 待检测环路的起始点ID
    // 用于防止同一路径上的重复访问和消息风暴
    // key: (起始点ID, 当前顶点ID)
    // value: 最短路径长度 (避免无限循环)
    private HashSet<Tuple<Object, Object>> visitedPaths;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Tuple<Object, Integer>> context, Object[] params) {
        this.context = context;
        if (params == null || params.length != 1) {
            throw new IllegalArgumentException("SingleNodeCycleDetection requires exactly one parameter: startVertexId.");
        }
        this.startVertexId = TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType());
        this.visitedPaths = new HashSet<>();
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Object, Integer>> messages) {
        if (context.getCurrentIterationId() == 1L) {
            // 第一轮迭代：只有起始点激活并向其邻居发送消息
            if (vertex.getId().equals(startVertexId)) {
                // 消息格式：Tuple<originId, currentPathLength>
                // 初始消息：(startVertexId, 1) 表示到达邻居的路径长度为 1
                sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH),
                    new Tuple<>(startVertexId, 1));
                // 将起始点自身添加到已访问路径，避免检测长度为0的“环路”
                visitedPaths.add(new Tuple<>(startVertexId, startVertexId));
            }
        } else {
            // 后续迭代：处理收到的消息
            while (messages.hasNext()) {
                Tuple<Object, Integer> message = messages.next();
                Object originId = message.f0();
                Integer pathLength = message.f1();

                // 检查是否已访问过此 (originId, 当前顶点ID) 路径，避免重复处理和无限循环
                Tuple<Object, Object> currentPath = new Tuple<>(originId, vertex.getId());
                if (visitedPaths.contains(currentPath)) {
                    continue; // 已经处理过这条路径，跳过
                }
                visitedPaths.add(currentPath); // 标记为已访问

                // 如果当前顶点是起始点，并且消息的起始点也是它，且路径长度大于1，则检测到环路
                // pathLength 应该 >= 2 (起始点 -> 邻居 -> ... -> 起始点)
                if (vertex.getId().equals(startVertexId) && originId.equals(startVertexId) && pathLength >= 2) {
                    context.take(ObjectRow.create(startVertexId, pathLength)); // 发现环路，输出结果 (起始点ID, 环路长度)
                    context.endVertex(); // 该顶点已完成任务，可以停止后续计算
                } else {
                    // 如果不是起始点或环路未形成，继续向邻居转发消息，路径长度加1
                    sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH),
                        new Tuple<>(originId, pathLength + 1));
                }
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // 无需特殊处理
    }

    /**
     * 定义算法的输出类型。
     * 输出包含环路起始点的ID和环路的长度。
     *
     * @param graphSchema 图的Schema信息
     * @return 包含环路ID和长度的StructType
     */
    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("cycle_start_id", graphSchema.getIdType(), false),
            new TableField("cycle_length", graphSchema.getPropertyType("INTEGER"), false)
        );
    }

    /**
     * 向给定边的目标顶点发送消息。
     *
     * @param edges   要遍历的边列表
     * @param message 要发送的消息
     */
    private void sendMessageToNeighbors(List<RowEdge> edges, Tuple<Object, Integer> message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}