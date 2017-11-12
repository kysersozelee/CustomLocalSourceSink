/*
 * Copyright [2017] [kysersoze.lee@gmail.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.kysersozelee.Sink;

import com.github.kysersozelee.Source.SubSource;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

@Slf4j
public class PubSink extends AbstractSink implements Configurable {
    private SinkCounter sinkCounter = new SinkCounter(PubSink.class.getName() + "_Counter");

    private static final String CONFIG_SUBSCRIBER_CLASS_NAME = "SubSource";
    private static final String CONFIG_MAX_REQUEST = "maxRequest";
    private static final String CONFIG_SUB_SOURCE_IDX = "subSourceIdx";

    //maxRequest는 채널의 채널의 transaction size를 초과 할 수 없음. transaction size를 벗어나기 때문에 에러 발생.
    private long maxRequest = Long.MAX_VALUE;

    //SubScriber를 가지고 있는 Source Class의 SimpleName
    private String subscriberClassName;
    //SubScriber의 index값. 해당 index는 pub-sub event 시에 해당 이벤트를 어느 소스에서 처리할지에 결정되는 값.
    private int subSourceIdx = 0;
    //Source에서 reflection을 이용해 가져온 Subscriber instance.
    private Subscriber<Event> eventSubscriber;

    //channel에서 taken한 event를 담아두는 array.
    private ArrayList<Event> reaminEventList = Lists.newArrayList();


    private final Publisher publisher = (subscriber) -> {
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long requestSize) {
                reaminEventList.stream().limit(requestSize).forEach(subscriber::onNext);
                subscriber.onComplete();
            }

            @Override
            public void cancel() {

            }
        });
    };

    @Override
    public void configure(Context context) {
        this.subscriberClassName = context.getString(CONFIG_SUBSCRIBER_CLASS_NAME, SubSource.class.getSimpleName());
        this.maxRequest = Objects.requireNonNull(context.getLong(CONFIG_MAX_REQUEST));
        this.subSourceIdx = Objects.requireNonNull(context.getInteger(CONFIG_SUB_SOURCE_IDX));
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();

            int size;
            for (size = 0; size < this.maxRequest; ++size) {
                try {
                    Event event = channel.take();
                    if (event == null) {
                        this.sinkCounter.incrementBatchEmptyCount();
                        break;
                    }

                    if (null == this.eventSubscriber) {
                        //reflection을 이용한 subscriber 등록
                        Class subscriberClass = Class.forName(this.subscriberClassName);
                        Field subscriberField = subscriberClass.getField("subscriberPerIdx");
                        subscriberField.setAccessible(true);
                        HashMap<Integer, Subscriber<Event>> subscriberPerIdx = (HashMap<Integer, Subscriber<Event>>) subscriberField.get(null);
                        this.eventSubscriber = subscriberPerIdx.get(this.subSourceIdx);
                        publisher.subscribe(this.eventSubscriber);
                    }
                    reaminEventList.add(event);
                } catch (ChannelException e) {
                    log.error("Exceeds transaction size. maxRequest:{}, batchSize:{}, error:{}", new Object[]{this.maxRequest, size, e.getMessage()});
                }
            }

            if (size < this.maxRequest) {
                this.sinkCounter.incrementBatchUnderflowCount();
            } else {
                this.sinkCounter.incrementBatchCompleteCount();
            }

            this.sinkCounter.addToEventDrainAttemptCount((long) size);
            transaction.commit();
            this.sinkCounter.addToEventDrainSuccessCount((long) size);
        } catch (Throwable var10) {
            transaction.rollback();
            log.error("Rpc Sink " + this.getName() + ": Unable to get event from channel " + channel.getName() + ". Exception follows.", var10);
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }

        return status;
    }


}