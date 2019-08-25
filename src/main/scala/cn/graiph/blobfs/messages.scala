package cn.graiph.blobfs

/**
  * Created by bluejoe on 2019/8/23.
  */
case class SendFileBlock(uuid: String, block: Array[Byte],
                         offset: Int, blockLength: Int,
                         totalLength: Long, blockIndex: Int) {

}

case class SendFileDiscard(uuid: String) {

}