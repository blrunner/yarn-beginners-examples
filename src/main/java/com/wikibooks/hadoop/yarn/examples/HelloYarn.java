/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wikibooks.hadoop.yarn.examples;

public class HelloYarn {
  private static final long MEGABYTE = 1024L * 1024L;

  public HelloYarn() {
    System.out.println("HelloYarn!");
  }

  public static long bytesToMegabytes(long bytes) {
    return bytes / MEGABYTE;
  }

  public void printMemoryStats() {
    long freeMemory = bytesToMegabytes(Runtime.getRuntime().freeMemory());
    long totalMemory = bytesToMegabytes(Runtime.getRuntime().totalMemory());
    long maxMemory = bytesToMegabytes(Runtime.getRuntime().maxMemory());

    System.out.println("The amount of free memory in the Java Virtual Machine: " + freeMemory);
    System.out.println("The total amount of memory in the Java virtual machine: " + totalMemory);
    System.out.println("The maximum amount of memory that the Java virtual machine: " + maxMemory);
  }

  public static void main(String[] args) {
    HelloYarn helloYarn = new HelloYarn();
    helloYarn.printMemoryStats();
  }
}
