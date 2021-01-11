/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.config.configcenter.ConfigChangeType;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.zookeeper.DataListener;
import org.apache.dubbo.remoting.zookeeper.EventType;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import static org.apache.dubbo.common.constants.CommonConstants.DOT_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;

/**
 * 事件监听
 */

public class CacheListener implements DataListener {
    /*
     * 路径的最小深度
     */
    private static final int MIN_PATH_DEPTH = 5;

    /**
     *  路径->监听器集合
     *  key是把path转化了 使用了 @pathToKey 转化的
     *  当key发生变化，会执行对应监听器的process方法
     */
    private Map<String, Set<ConfigurationListener>> keyListeners = new ConcurrentHashMap<>();
    private CountDownLatch initializedLatch;
    private String rootPath;

    public CacheListener(String rootPath, CountDownLatch initializedLatch) {
        this.rootPath = rootPath;
        this.initializedLatch = initializedLatch;
    }

    public void addListener(String key, ConfigurationListener configurationListener) {
        Set<ConfigurationListener> listeners = this.keyListeners.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>());
        listeners.add(configurationListener);
    }

    public void removeListener(String key, ConfigurationListener configurationListener) {
        Set<ConfigurationListener> listeners = this.keyListeners.get(key);
        if (listeners != null) {
            listeners.remove(configurationListener);
        }
    }

    /**
     * This is used to convert a configuration nodePath into a key
     * TODO doc
     *
     * @param path
     * @return key (nodePath less the config root path)
     */
    private String pathToKey(String path) {
        if (StringUtils.isEmpty(path)) {
            return path;
        }

        /*
         *   替换 1）@rootPath/ 替换为空串，因为都是一样的
         *       2）全部的 / 替换为.
         */
        String groupKey = path.replace(rootPath + PATH_SEPARATOR, "").replaceAll(PATH_SEPARATOR, DOT_SEPARATOR);
        return groupKey.substring(groupKey.indexOf(DOT_SEPARATOR) + 1);
    }

    /*
     *  根据path获取所属group信息
     */
    private String getGroup(String path) {
        if (!StringUtils.isEmpty(path)) {
            // 获取根节点的起始位置 也就是判断当前路径是否在监听范围内
            int beginIndex = path.indexOf(rootPath + PATH_SEPARATOR);
            if (beginIndex > -1) {
                // 获取从根节点开始 第一个/的位置
                int endIndex = path.indexOf(PATH_SEPARATOR, beginIndex);
                if (endIndex > beginIndex) {
                    // 根节点中第一个/之前的内容，为group信息
                    return path.substring(beginIndex, endIndex);
                }
            }
        }
        return path;
    }


    /**
     *  数据变化监听实现
     * @param path  监听路径
     * @param value 新值
     * @param eventType 事件类型
     */
    @Override
    public void dataChanged(String path, Object value, EventType eventType) {
        if (eventType == null) {
            return;
        }

        if (eventType == EventType.INITIALIZED) {
            initializedLatch.countDown();
            return;
        }

        // 路径为空 or 没有新值，但是不是删除操作，无需处理
        if (path == null || (value == null && eventType != EventType.NodeDeleted)) {
            return;
        }

        // TODO We only care the changes happened on a specific path level, for example
        //  /dubbo/config/dubbo/configurators, other config changes not in this level will be ignored,

        // 路径深度不能小于最小值
        if (path.split("/").length >= MIN_PATH_DEPTH) {
            // 路径转key
            String key = pathToKey(path);
            ConfigChangeType changeType;
            // 确定事件类型 目前只会处理 1）节点新增 2）节点删除 3）节点修改
            switch (eventType) {
                case NodeCreated:
                    changeType = ConfigChangeType.ADDED;
                    break;
                case NodeDeleted:
                    changeType = ConfigChangeType.DELETED;
                    break;
                case NodeDataChanged:
                    changeType = ConfigChangeType.MODIFIED;
                    break;
                default:
                    return;
            }

            // 构建配置变更世界
            ConfigChangedEvent configChangeEvent = new ConfigChangedEvent(key, getGroup(path), (String) value, changeType);
            Set<ConfigurationListener> listeners = keyListeners.get(path);
            if (CollectionUtils.isNotEmpty(listeners)) {
                // 执行绑定监听器的行为
                listeners.forEach(listener -> listener.process(configChangeEvent));
            }
        }
    }
}
