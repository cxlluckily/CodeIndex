package me.iroohom.pattern.builder.after;

public class HighBuilding extends HouseBuilder{
    @Override
    public void buildBase() {
        System.out.println("build the base of a high building...");
    }

    @Override
    public void buildWall() {
        System.out.println("build a wall of a high building...");

    }

    @Override
    public void buildRoof() {
        System.out.println("build the roof of a high building...");
    }
}
