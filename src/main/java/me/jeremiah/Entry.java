package me.jeremiah;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import me.jeremiah.utils.EntryGeneratorUtils;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

@AllArgsConstructor
@Setter
@Getter
public class Entry {

  private final int id;
  private String firstName;
  private char middleInitial;
  private String lastName;
  private int age;
  private double netWorth;


  public Entry(int id, byte[] data) {
    this.id = id;
    ByteBuffer buffer = ByteBuffer.wrap(data);

    int firstNameLength = buffer.get();
    byte[] firstNameBytes = new byte[firstNameLength];
    buffer.get(firstNameBytes);
    this.firstName = new String(firstNameBytes);

    this.middleInitial = buffer.getChar();

    int lastNameLength = buffer.get();
    byte[] lastNameBytes = new byte[lastNameLength];
    buffer.get(lastNameBytes);
    this.lastName = new String(lastNameBytes);

    this.age = buffer.getInt();
    this.netWorth = buffer.getDouble();
  }

  public Entry(int id) {
    this(id,
      EntryGeneratorUtils.generateFirstName(),
      EntryGeneratorUtils.generateMiddleInitial(),
      EntryGeneratorUtils.generateLastName(),
      EntryGeneratorUtils.generateAge(),
      EntryGeneratorUtils.generateNetWorth()
    );
  }

  public @NotNull Document toDocument() {
    return new Document()
      .append("id", id)
      .append("first_name", firstName)
      .append("middle_initial", String.valueOf(middleInitial))
      .append("last_name", lastName)
      .append("age", age)
      .append("net_worth", netWorth);
  }

  public @NotNull Document toByteDocument() {
    return new Document()
      .append("id", id)
      .append("data", this.bytes());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof Entry entry))
      return false;
    return id == entry.id &&
      firstName.equals(entry.firstName) &&
      middleInitial == entry.middleInitial &&
      lastName.equals(entry.lastName) &&
      age == entry.age &&
      netWorth == entry.netWorth;
  }

  public byte[] bytes() {
    byte[] firstNameBytes = firstName.getBytes();
    byte[] lastNameBytes = lastName.getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(
      1 +                     // Length of firstName (int)
      firstNameBytes.length + // Actual firstName bytes
      2 +                     // middleInitial (char)
      1 +                     // Length of lastName (int)
      lastNameBytes.length +  // Actual lastName bytes
      4 +                     // age (int)
      8                       // netWorth (double)
    );

    buffer
      .put((byte) firstNameBytes.length)  // Length of firstName
      .put(firstNameBytes)            // firstName bytes
      .putChar(middleInitial)         // middleInitial
      .put((byte) lastNameBytes.length)   // Length of lastName
      .put(lastNameBytes)             // lastName bytes
      .putInt(age)                    // age
      .putDouble(netWorth);           // netWorth

    return buffer.array();
  }

}
