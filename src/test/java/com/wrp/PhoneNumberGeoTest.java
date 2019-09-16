package com.wrp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.wrp.PhoneNumberGeo;
import com.wrp.PhoneNumberInfo;
import org.junit.Test;

/**
 * Created by wrp on 16/10/12.
 */
public class PhoneNumberGeoTest {

  @Test public void testNormalCase() throws Exception {
    PhoneNumberGeo phoneNumberGeo = new PhoneNumberGeo();

    PhoneNumberInfo phoneNumberInfo = phoneNumberGeo.lookup("13006150000");
    assertEquals(
        "PhoneNumberInfo{phoneNumber='13006150000', province='湖北', city='武汉', zipCode='430000', areaCode='027', phoneType='联通'}",
        phoneNumberInfo.toString());
    System.out.println(phoneNumberInfo.getProvince());

    phoneNumberInfo = phoneNumberGeo.lookup("12345678901");
    assertNull(phoneNumberInfo);

    phoneNumberInfo = phoneNumberGeo.lookup("99999999999");
    assertNull(phoneNumberInfo);

    phoneNumberInfo = phoneNumberGeo.lookup("abcdefghijk");
    assertNull(phoneNumberInfo);
  }
}