package tg.example

case class Config(action: String = "all",
                  path: String = "",
                  graphDir: String = "",
                  resultDir: String = "",
                  wl: Long = 0, sl: Long = 0,
                  numWindow: Int = 1)
