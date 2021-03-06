package com.data.factory.adapters

import com.data.factory.exceptions.EncoderException
import com.data.factory.ports.Encoder

class Base64Encoder extends Encoder with Serializable{
  import java.util.Base64
  import java.nio.charset.StandardCharsets
  def encode(input: String): String = {
    if (input == null){
      throw new EncoderException("Input cannot be null")

    }
    else{
      if(input.isEmpty()){
        throw new EncoderException("Input cannot be empty")

      }
      else{
        return Base64.getEncoder.encodeToString(input.getBytes())
      }
    }
  }
  def decode(input: String): String = {
    if (input == null){
      throw new EncoderException("Input cannot be null")

    }
    else{
      if(input.isEmpty()){
        throw new EncoderException("Input cannot be empty")

      }
      else{
        val bytes = Base64.getDecoder().decode(input)
        return new String(bytes)
      }
    }
  }

}
