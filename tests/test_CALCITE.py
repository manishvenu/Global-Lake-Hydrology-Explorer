from GLHE.CALCITE import pubsub, events


class TestPubSub:
    def test_pubsub(self):
        self.subscribe_event()
        self.publish_event()
        assert "TestEvent" in pubsub.EventBus.topics

    def recieve_event(self, msg):
        assert msg.name == "Test Message"

    def subscribe_event(self):
        pubsub.EventBus.Subscribe(pubsub.EventBus, "TestEvent", self.recieve_event)

    def publish_event(self):
        pubsub.EventBus.Publish(pubsub.EventBus, events.TestEvent("Test Message"))
