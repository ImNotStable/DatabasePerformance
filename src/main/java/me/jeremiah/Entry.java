package me.jeremiah;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import me.jeremiah.utils.EntryGeneratorUtils;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.io.*;

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
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
      this.id = id;
      this.firstName = dis.readUTF();
      this.middleInitial = dis.readChar();
      this.lastName = dis.readUTF();
      this.age = dis.readInt();
      this.netWorth = dis.readDouble();
    } catch (IOException e) {
      throw new RuntimeException("Error deserializing Entry from bytes", e);
    }
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
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeUTF(firstName);
      dos.writeChar(middleInitial);
      dos.writeUTF(lastName);
      dos.writeInt(age);
      dos.writeDouble(netWorth);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error serializing Entry to bytes", e);
    }
  }

}
