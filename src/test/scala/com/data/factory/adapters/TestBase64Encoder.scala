package com.data.factory.adapters

import com.data.factory.exceptions.EncoderException
import com.falabella.products.models._
import com.falabella.products.ports._
import com.falabella.products.exceptions._
import org.scalatest.Assertions._
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalamock.scalatest.MockFactory

class TestBase64Encoder extends FlatSpec with BeforeAndAfter {

    "constructor" should "return valid object" in {
        //Arrange

        //Act
        val encoder: Base64Encoder = new Base64Encoder()

        //Assert
        assert(encoder.isInstanceOf[Base64Encoder])
    }

    "encode" should "return encoded in base 64 string if receive valid input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = """{"inputPath": "/Users/mirckogalleaniencina/Development/India/cl_falabella_re_encrypt_spark"}"""
        val expected: String = "eyJpbnB1dFBhdGgiOiAiL1VzZXJzL21pcmNrb2dhbGxlYW5pZW5jaW5hL0RldmVsb3BtZW50L0luZGlhL2NsX2ZhbGFiZWxsYV9yZV9lbmNyeXB0X3NwYXJrIn0="
        
        //Act
        val actual: String = encoder.encode(input)
        
        //Assert
        assert(actual == expected)
    }
    
    it should "return EncoderException when receive empty input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = ""
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.encode(input)
            }
        val expected = "Input cannot be empty"

        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return EncoderException when receive null input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = null
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.encode(input)
            }
        val expected = "Input cannot be null"

        //Assert
        assert(actual.getMessage == expected)
    }

    "decode" should "return decoded in base 64 string if receive valid input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = "eyJpbnB1dFBhdGgiOiAiL1VzZXJzL21pcmNrb2dhbGxlYW5pZW5jaW5hL0RldmVsb3BtZW50L0luZGlhL2NsX2ZhbGFiZWxsYV9yZV9lbmNyeXB0X3NwYXJrIn0="
        val expected: String = """{"inputPath": "/Users/mirckogalleaniencina/Development/India/cl_falabella_re_encrypt_spark"}"""
        
        //Act
        val actual: String = encoder.decode(input)
        
        //Assert
        assert(actual == expected)
    }
    
    it should "return EncoderException when receive empty input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = ""
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.decode(input)
            }
        val expected = "Input cannot be empty"

        //Assert
        assert(actual.getMessage == expected)
    }

    it should "return EncoderException when receive null input" in {
        
        //Arrange
        val encoder: Base64Encoder = new Base64Encoder()
        val input: String = null
  
        //Act
        val actual =
            intercept[EncoderException] {
                val result: String = encoder.decode(input)
            }
        val expected = "Input cannot be null"

        //Assert
        assert(actual.getMessage == expected)
    }
}