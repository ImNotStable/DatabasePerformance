package me.jeremiah.testing;

import java.util.Iterator;

public class InstructionSet implements Iterator<TestInstructions>, Cloneable {

  private int index = 0;
  private final TestInstructions[] instructions;

  private InstructionSet(TestInstructions... instructions) {
    this.instructions = instructions;
  }

  @Override
  public boolean hasNext() {
    return index < instructions.length;
  }

  @Override
  public TestInstructions next() {
    return instructions[index++];
  }

  @Override
  public InstructionSet clone() {
    return new InstructionSet(instructions);
  }

}
