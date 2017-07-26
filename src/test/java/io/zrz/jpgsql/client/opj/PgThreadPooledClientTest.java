package io.zrz.jpgsql.client.opj;

import org.junit.Test;

import com.google.common.collect.Sets;

import io.reactivex.Flowable;
import io.zrz.jpgsql.client.NotifyMessage;

public class PgThreadPooledClientTest {

  @Test
  public void test() throws InterruptedException {
    final PgThreadPooledClient client = PgThreadPooledClient.create("localhost", "saasy");
    final Flowable<NotifyMessage> notifies = client.notifications(Sets.newHashSet("xxx"));
    notifies.blockingSubscribe(notify -> {
      System.err.println(notify);
    });
  }

}
