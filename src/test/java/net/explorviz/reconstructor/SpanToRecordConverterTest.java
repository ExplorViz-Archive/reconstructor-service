package net.explorviz.reconstructor;

import java.time.Instant;
import net.explorviz.avro.SpanStructure;
import net.explorviz.avro.Timestamp;

import net.explorviz.avro.landscape.flat.Application;
import net.explorviz.avro.landscape.flat.Node;
import net.explorviz.avro.landscape.flat.LandscapeRecord;
import net.explorviz.reconstructor.stream.SpanToRecordConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanToRecordConverterTest {

  private SpanToRecordConverter converter;

  private SpanStructure span;
  private LandscapeRecord record;

  @BeforeEach
  void setUp() {
    this.converter = new SpanToRecordConverter();
    final Instant now = Instant.now();
    final String token = "tok";
    final String hostname = "Host";
    final String hostIp = "1.2.3.4";
    final String appName = "Test App";
    final String appPid = "1234";
    final String appLang = "java";
    final String hashCode = "a387988168c607be0b2d886e75c85cb0f2f44ed41d45a1d800cdc857c04e98ae";


    this.span = SpanStructure.newBuilder()
        .setSpanId("id")
        .setLandscapeToken(token)
        .setHashCode(hashCode)
        .setTimestamp(new Timestamp(now.getEpochSecond(), now.getNano()))
        .setHostname(hostname)
        .setHostIpAddress(hostIp)
        .setAppName(appName)
        .setAppPid(appPid)
        .setAppLanguage(appLang)
        .setFullyQualifiedOperationName("foo.bar.TestClass.testMethod()")
        .setHashCode("12345")
        .build();

    this.record = LandscapeRecord.newBuilder()
        .setLandscapeToken(token)
        .setHashCode(hashCode)
        .setTimestamp(now.toEpochMilli())
        .setNode(new Node(hostIp, hostname))
        .setApplication(new Application(appName, appPid, appLang))
        .setPackage$("foo.bar")
        .setClass$("TestClass")
        .setMethod("testMethod()")
        .setHashCode("12345")
        .build();

  }

  @Test
  public void convert() {
    final LandscapeRecord got = this.converter.toRecord(this.span);
    Assertions.assertEquals(got, this.record, "Converted records does not match expected");
  }

}
