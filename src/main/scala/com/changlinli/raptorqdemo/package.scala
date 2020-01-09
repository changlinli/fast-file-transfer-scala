package com.changlinli

package object raptorqdemo {

  implicit class CustomStdOps[A](x: A) {
    def |>[B](f: A => B): B = f(x)
  }

}
