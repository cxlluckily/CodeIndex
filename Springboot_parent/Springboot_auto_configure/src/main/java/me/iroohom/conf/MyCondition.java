package me.iroohom.conf;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * @ClassName: MyCondition
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/14 16:40
 * @Software: IntelliJ IDEA
 */
public class MyCondition implements Condition {
    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        //判断的条件：如果返回true，表示让对象放置到容器中，如果false，表示不允许
        return true;
    }
}
