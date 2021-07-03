package me.iroohom.pattern.builder.before;

public abstract class AbstractHouse {
    //打地基
    public abstract void buildBase();

    //砌墙
    public abstract void buildWall();

    //封顶
    public abstract void roofed();


    public void build() {
        buildBase();
        buildWall();
        roofed();
    }
}
