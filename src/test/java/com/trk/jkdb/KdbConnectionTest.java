package com.trk.jkdb;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.junit.Test;

public class KdbConnectionTest {

	private final static char[] HEX = "0123456789ABCDEF".toCharArray();

	public static String toHex(byte[] bytes) {
		final char[] h = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			h[j * 2] = HEX[v >>> 4];
			h[j * 2 + 1] = HEX[v & 0x0F];
		}
		return new String(h);
	}

	@Test
	public void testSerializeLocalDate() {
		KdbConnection c = new KdbConnection();
		byte[] buf = new byte[4];
		c.B = buf;
		LocalDate date = LocalDate.of(2018, 9, 19);
		c.w(date);
		System.out.println("Serialization Buffer:" + toHex(buf));
		c.b = buf;
		// -8!2018.09.19
		// 0x010000000d000000f2b41a0000
		// ------
		// 0x01
		// 01 - little endian
		// 00 - async
		// 0000
		// 0d000000 msg length
		// f2 - type (-14)
		// b41a0000
		// Note: 0xb41a is little endian 1A B4 which is 6836
		// 6836 is epoch days value for 2018.09.19 - kx magic offset of 10957
		LocalDate deserialized = c.rd();
		assertEquals(date, deserialized);
	}

	@Test
	public void testLocalDateTimeFromNanos() {
		long nanos = 1571667445187932000L;
		LocalDateTime dt1 = LocalDateTime.ofInstant(
				Instant.ofEpochSecond((long) nanos / 1_000_000_000L, nanos % 1_000_000_000L), ZoneOffset.UTC);

		long kdbTime = 624982645187932000L;
		long epoch = 946684800000000000L;
		long adjusted = epoch + kdbTime;
		LocalDateTime dt2 = LocalDateTime.ofInstant(
				Instant.ofEpochSecond((long) adjusted / 1_000_000_000L, adjusted % 1_000_000_000L), ZoneOffset.UTC);

		assertEquals(dt1, dt2);
	}

	@Test
	public void testSerializeLocalDateTme() {
		KdbConnection c = new KdbConnection();
		byte[] buf = new byte[8];
		c.B = buf;

		// -8!2019.10.21D14:17:25.187932000
		// 0x0100000011000000f460ef358d6762ac08
		// 0x01 - little endian
		// 0x00 - async
		// 0x0000
		// 11000000 - msg length 0x11=17 bytes
		// f4 - type (-12)
		// 60ef358d6762ac08 8-byte timestamp value
		// this is 0x8ac62678d35ef60 in little-endian
		// which is decimal ‭624982645187932000
		// which is q)`long$p
		// 624982645187932000
		// ie nanoseconds since 2000.01.01

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd'D'HH:mm:ss.SSSSSSSSS");
		LocalDateTime dt = (LocalDateTime) LocalDateTime.parse("2019.10.21D14:17:25.187932000", formatter);
		c.w(dt);
		System.out.println("Serialization Buffer:" + toHex(buf));
		c.b = buf;
		LocalDateTime deserialized = c.rp();
		assertEquals(dt, deserialized);

	}

}
