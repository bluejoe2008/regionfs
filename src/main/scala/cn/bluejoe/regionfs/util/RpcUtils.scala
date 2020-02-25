package cn.bluejoe.regionfs.util

import net.neoremind.kraps.rpc.RpcAddress

/**
  * Created by bluejoe on 2020/2/24.
  */
object RpcAddressUtils {
  def fromString(url: String, separtor: String = ":") = {
    val pair = url.split(separtor)
    RpcAddress(pair(0), pair(1).toInt)
  }
}
