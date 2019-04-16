/**
 * Copyright 2009-2018 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ibatis.plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Clinton Begin
 * <p>
 * 拦截器 Interceptor 链
 */
public class InterceptorChain {

    /**
     * 拦截器数组
     */
    private final List<Interceptor> interceptors = new ArrayList<>();

    /**
     * 应用所有拦截器到指定目标对象：
     * 被 Configuration 的如下四处方法中调用：
     *      一共可以有四种目标对象类型可以被拦截：1）Executor；2）StatementHandler；3）ParameterHandler；4）ResultSetHandler
     * eg: http://static2.iocoder.cn/images/MyBatis/2020_03_18/03.png
     */
    public Object pluginAll(Object target) {
        for (Interceptor interceptor : interceptors) {
            target = interceptor.plugin(target);
        }
        return target;
    }

    // 添加拦截器
    public void addInterceptor(Interceptor interceptor) {
        interceptors.add(interceptor);
    }

    public List<Interceptor> getInterceptors() {
        return Collections.unmodifiableList(interceptors);
    }

}
