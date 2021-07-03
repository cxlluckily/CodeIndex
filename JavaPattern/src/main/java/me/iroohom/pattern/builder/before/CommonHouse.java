package me.iroohom.pattern.builder.before;

public class CommonHouse extends AbstractHouse{
    @Override
    public void buildBase() {
        System.out.println("Building the base of a common house...");
    }

    @Override
    public void buildWall() {
        System.out.println("Building a wall for a common house...");
    }

    @Override
    public void roofed() {
        System.out.println("Roofing a roof of a common house...");
    }
}
