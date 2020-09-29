package io.zrz.jpgsql.client.opj;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.reactivex.rxjava3.core.Flowable;
import io.zrz.jpgsql.client.NotifyMessage;

public class PgThreadPooledClientTest {

  @Ignore
  @Test
  public void test() throws InterruptedException {
    final PgThreadPooledClient client = PgThreadPooledClient.create("localhost", "saasy");
    final Flowable<NotifyMessage> notifies = client.notifications(Sets.newHashSet("xxx"));
    notifies.blockingSubscribe(notify -> {
      System.err.println(notify);
    });
  }

}
