package me.jeremiah;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import me.jeremiah.utils.EntryGeneratorUtils;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@AllArgsConstructor
@Setter
@Getter
public class Entry implements DatabaseSerializable {

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

  @Override
  public void serializeInsert(@NotNull PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
    preparedStatement.setString(2, firstName);
    preparedStatement.setString(3, String.valueOf(middleInitial));
    preparedStatement.setString(4, lastName);
    preparedStatement.setInt(5, age);
    preparedStatement.setDouble(6, netWorth);
  }

  @Override
public void serializeUpdate(@NotNull PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setString(1, firstName);
    preparedStatement.setString(2, String.valueOf(middleInitial));
    preparedStatement.setString(3, lastName);
    preparedStatement.setInt(4, age);
    preparedStatement.setDouble(5, netWorth);
    preparedStatement.setInt(6, id);
  }

  @Override
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
    if (obj == null || getClass() != obj.getClass()) return false;
    Entry entry = (Entry) obj;
    return id == entry.id &&
      middleInitial == entry.middleInitial &&
      firstName.equals(entry.firstName) &&
      lastName.equals(entry.lastName) &&
      age == entry.age &&
      netWorth == entry.netWorth;
  }

  @Override
  public byte[] bytes() {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeInt(id);
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

  @Override
  public String toString() {
    return firstName + "," + middleInitial + "," + lastName + "," + age + "," + netWorth;
  }

}
