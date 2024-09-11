package me.jeremiah.utils;

import java.util.concurrent.ThreadLocalRandom;

public class EntryGeneratorUtils {

  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  private static final String[] FIRST_NAMES = {
    "Aaron", "Abby", "Acelyn", "Adam", "Adrian", "Aiden", "Ainsley", "Alana", "Alex", "Alexa",
    "Bella", "Ben", "Benny", "Bianca", "Bill", "Billy", "Bobby", "Bonnie", "Brad", "Bradley",
    "Caleb", "Cameron", "Cara", "Carla", "Carly", "Carmen", "Carol", "Caroline", "Carrie", "Carter",
    "Daisy", "Dale", "Dana", "Daniel", "Danny", "Daphne", "Darla", "Darlene", "Darrell", "Darren",
    "Eddie", "Eden", "Edgar", "Edith", "Edna", "Edwin", "Eileen", "Elaine", "Eleanor", "Elena",
    "Faye", "Felix", "Fern", "Fiona", "Flora", "Floyd", "Forrest", "Frances", "Frank", "Frankie",
    "Gabe", "Gabriel", "Gail", "Gale", "Galen", "Garry", "Gary", "Gavin", "Gayle", "Gene", "George",
    "Haley", "Hank", "Hanna", "Hannah", "Harley", "Harold", "Harriet", "Harry", "Harvey", "Hazel",
    "Ian", "Ike", "Imogene", "Ina", "India", "Inez", "Ira", "Irene", "Iris", "Irma", "Isaac", "Isabel",
    "Jack", "Jackie", "Jacob", "Jade", "Jaden", "Jagger", "Jaime", "Jake", "James", "Jamie", "Jan",
    "Kade", "Kaden", "Kai", "Kaitlyn", "Kaleb", "Kali", "Kara", "Karen", "Kari", "Karl", "Karla",
    "Lacey", "Lacy", "Ladonna", "Laila", "Lainey", "Lana", "Lance", "Landon", "Lane", "Lara", "Larry",
    "Mabel", "Mack", "Maddie", "Maddox", "Maddy", "Madeline", "Madelyn", "Madison", "Mae", "Maeve",
    "Nadia", "Nadine", "Nancy", "Nanette", "Naomi", "Natalia", "Natalie", "Nate", "Nathan", "Nathaniel",
    "Olive", "Oliver", "Olivia", "Ollie", "Omar", "Ophelia", "Ora", "Orville", "Oscar", "Otis", "Otto",
    "Pablo", "Paige", "Paisley", "Pam", "Pamela", "Pansy", "Paris", "Parker", "Pat", "Patricia", "Patsy",
    "Quentin", "Quincy", "Quinn", "Quinton", "Quintin", "Quinton", "Quintrell", "Quintrell", "Quintrell",
    "Rachael", "Rachel", "Rachelle", "Rae", "Rafael", "Ralph", "Ramona", "Randall", "Randy", "Raven",
    "Sable", "Sabrina", "Sadie", "Sage", "Sally", "Sam", "Samantha", "Sammy", "Sandra", "Sandy", "Sara",
    "Tad", "Talia", "Tamara", "Tammy", "Tanner", "Tara", "Tasha", "Tate", "Taylor", "Ted", "Teddy", "Terry",
    "Ula", "Ulysses", "Una", "Uriah", "Uriel", "Ursula", "Usher", "Uta", "Ute", "Uwe", "Uyless",
    "Val", "Valarie", "Valeria", "Valerie", "Valery", "Van", "Vance", "Vera", "Vern", "Verna", "Vernon",
    "Wade", "Waldo", "Walker", "Wallace", "Wally", "Walt", "Walter", "Wanda", "Warren", "Wayne", "Wendy",
    "Xander", "Xavier", "Xena", "Xenia", "Ximena", "Xiomara", "Xochitl", "Xyla", "Xylia", "Xylina", "Xylona",
    "Yael", "Yahir", "Yamilet", "Yamileth", "Yara", "Yaretzi", "Yasmin", "Yasmine", "Yazmin", "Yessenia", "Yolanda",
    "Zach", "Zachariah", "Zachary", "Zachery", "Zack", "Zackary", "Zackery", "Zada", "Zahara", "Zaiden", "Zain"
  };

  private static final char[] MIDDLE_INITIALS = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
    'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
    'U', 'V', 'W', 'X', 'Y', 'Z'
  };

  private static final String[] LAST_NAMES = {
    "Allen", "Anderson", "Andrews", "Armstrong", "Arnold", "Austin", "Adams", "Adkins", "Aguilar", "Aguirre",
    "Baker", "Baldwin", "Ball", "Ballard", "Banks", "Barber", "Barker", "Barnes", "Barnett", "Barrett", "Barton",
    "Carter", "Cassidy", "Cervantes", "Chambers", "Chan", "Chandler", "Chang", "Chapman", "Chavez", "Chen",
    "Davies", "Davis", "Dawson", "Day", "Dean", "Delacruz", "Delaney", "Deleon", "Delgado", "Dennis", "Diaz",
    "Eaton", "Edwards", "Elliott", "Ellis", "Emerson", "England", "English", "Erickson", "Espinoza", "Estes",
    "Ferguson", "Fernandez", "Ferrell", "Fields", "Figueroa", "Finley", "Fischer", "Fisher", "Fitzgerald", "Fleming",
    "Garcia", "Garner", "Garrett", "Garrison", "Garza", "Gibson", "Gilbert", "Giles", "Gill", "Gillespie", "Gilliam",
    "Hale", "Hall", "Hamilton", "Hammond", "Hampton", "Hancock", "Haney", "Hansen", "Hanson", "Hardin", "Harding",
    "Ingram", "Irwin", "Isom", "Ivey", "Ibarra", "Ireland", "Irving", "Isaacs", "Isaac", "Isaiah", "Isabella",
    "Jackson", "Jacobs", "Jacobson", "James", "Jarvis", "Jefferson", "Jenkins", "Jennings", "Jensen", "Jimenez",
    "Kane", "Kaufman", "Keller", "Kelley", "Kelly", "Kemp", "Kennedy", "Kent", "Kerr", "Key", "Kidd", "Kim", "King",
    "Lamb", "Lambert", "Landry", "Lane", "Lang", "Langley", "Lara", "Larsen", "Larson", "Lawrence", "Lawson", "Le",
    "Mack", "Madden", "Maddox", "Maldonado", "Malone", "Mann", "Manning", "Marks", "Marquez", "Marsh", "Marshall",
    "Nash", "Nava", "Navarro", "Neal", "Nelson", "Newman", "Nguyen", "Nichols", "Nicholson", "Nielsen", "Nieves",
    "Odom", "Odonnell", "Oliver", "Olsen", "Olson", "Oneal", "Oneill", "Orozco", "Orr", "Ortega", "Ortiz", "Osborn",
    "Pace", "Pacheco", "Padilla", "Page", "Palmer", "Park", "Parker", "Parks", "Parrish", "Parsons", "Patel", "Patrick",
    "Quinn", "Quintana", "Quintero", "Quiroz", "Quist", "Quintanilla", "Quinones", "Qualls", "Quinlan", "Quirk", "Quinney",
    "Ramirez", "Ramos", "Ramsey", "Randall", "Randolph", "Rangel", "Rasmussen", "Ratliff", "Ray", "Raymond", "Reed",
    "Santana", "Santiago", "Santos", "Sargent", "Saunders", "Savage", "Sawyer", "Schaefer", "Schmidt", "Schmitt", "Schneider",
    "Tate", "Taylor", "Terrell", "Terry", "Thomas", "Thompson", "Thornton", "Tillman", "Todd", "Torres", "Townsend", "Tran",
    "Underwood", "Valdez", "Valencia", "Valentine", "Valenzuela", "Vance", "Vang", "Vargas", "Vasquez", "Vaughan", "Vaughn",
    "Vazquez", "Vega", "Velasquez", "Velazquez", "Velez", "Villarreal", "Villegas", "Vincent", "Vinson", "Vogel", "Vogt",
    "Wagner", "Walker", "Wall", "Wallace", "Waller", "Walls", "Walsh", "Walter", "Walters", "Walton", "Ward", "Ware", "Warner",
    "Ximenes", "Xiong", "Xu", "Xue", "Xuereb", "Xylander", "Xylas", "Xylina", "Xylona", "Xyloportas", "Xylos", "Xylouris",
    "Ybarra", "Yee", "Yi", "Yilmaz", "Yoder", "Yoon", "York", "Yost", "Young", "Yu", "Yuan", "Yusuf", "Yzaguirre", "Yates",
    "Zamora", "Zapata", "Zaragoza", "Zavala", "Zayas", "Zeller", "Zepeda", "Zhang", "Zhao", "Zhou", "Zimmerman", "Zuniga"
  };

  public static String generateFullName() {
    return generateFirstName() + " " + generateMiddleInitial() + ". " + generateLastName();
  }

  public static String generateFirstName() {
    return FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)];
  }

  public static char generateMiddleInitial() {
    return MIDDLE_INITIALS[RANDOM.nextInt(MIDDLE_INITIALS.length)];
  }

  public static String generateLastName() {
    return LAST_NAMES[RANDOM.nextInt(LAST_NAMES.length)];
  }

  public static int generateAge() {
    return RANDOM.nextInt(13, 120);
  }

  public static double generateNetWorth() {
    return RANDOM.nextDouble(-100_000_000_000.0, 100_000_000_000.0);
  }

}
