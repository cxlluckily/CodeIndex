package me.iroohom.pattern.builder.after;

public class Client {
    public static void main(String[] args) {
        CommonHouse commonHouse = new CommonHouse();
        HouseDirector houseDirector = new HouseDirector(commonHouse);

        //盖房子并返回
        House house = houseDirector.constructHouse();

        HighBuilding highBuilding = new HighBuilding();
        //重置导演
        houseDirector.setHouseBuilder(highBuilding);
        //盖房子并返回
        House aHighBuilding = houseDirector.constructHouse();
    }
}
