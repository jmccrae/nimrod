package nimrod

/** Monitors the progress of some task */
trait ProgressMonitor {
  /** Set the number of pips in the current stage
   * @param n The current number of steps, or zero for unknown
   */
  def reset(n : Int)
  /** Indicate some 'progress' has been made */
  def pip
  /** Create a monitor that only reports every n pips */
  def %(n : Int) = new ProgressMonitor {
    var pipsSeen = 0
    def reset(m : Int) = ProgressMonitor.this.reset(m / n)
    def pip = {
      pipsSeen += 1
      if(pipsSeen % n == 0) {
        ProgressMonitor.this.pip
      }
    }
  }
}

/** Ignores all progress */
object NullProgressMonitor extends ProgressMonitor {
  def reset(n : Int) { }
  def pip { }
}

/** Connects to a messenger */
class MessengerProgressMonitor(messenger : Messenger) extends ProgressMonitor {
  def reset(n : Int) = messenger.monitorReset(n)
  def pip = messenger.pip
}
