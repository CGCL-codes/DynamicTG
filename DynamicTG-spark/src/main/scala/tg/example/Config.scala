package tg.example

case class Config(path: String = null,
                  graphDir: String = null,
                  wl: Long = 0, sl: Long = 0,
                  numWindow: Int = 1,
                  isWrite: Boolean = false)
