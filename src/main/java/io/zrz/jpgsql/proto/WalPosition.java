// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.proto;

public final class WalPosition {
  private final long index;
  private final long sequence;

  public String toString() {
    return String.format("%s/%s", Long.toHexString(index), Long.toHexString(sequence));
  }

  public static WalPosition of(long timeline, long bytes) {
    return new WalPosition(timeline, bytes);
  }

  public static WalPosition fromString(String string) {
    int idx = string.indexOf('/');
    return of(Long.parseLong(string.substring(0, idx), 16), Long.parseLong(string.substring(idx + 1), 16));
  }

  @java.lang.SuppressWarnings("all")
  public WalPosition(final long index, final long sequence) {
    this.index = index;
    this.sequence = sequence;
  }

  @java.lang.SuppressWarnings("all")
  public long getIndex() {
    return this.index;
  }

  @java.lang.SuppressWarnings("all")
  public long getSequence() {
    return this.sequence;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public boolean equals(final java.lang.Object o) {
    if (o == this) return true;
    if (!(o instanceof WalPosition)) return false;
    final WalPosition other = (WalPosition) o;
    if (this.getIndex() != other.getIndex()) return false;
    if (this.getSequence() != other.getSequence()) return false;
    return true;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final long $index = this.getIndex();
    result = result * PRIME + (int) ($index >>> 32 ^ $index);
    final long $sequence = this.getSequence();
    result = result * PRIME + (int) ($sequence >>> 32 ^ $sequence);
    return result;
  }
}
