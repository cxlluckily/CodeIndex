package me.iroohom.pattern.builder.after;

public class CommonHouse extends HouseBuilder{
    @Override
    public void buildBase() {
        System.out.println("build the base 5 meters...");
    }

    @Override
    public void buildWall() {
        System.out.println("build the wall 5 meters...");

    }

    @Override
    public void buildRoof() {
        System.out.println("build the roof so high...");
    }
}
