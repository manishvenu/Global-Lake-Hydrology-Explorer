import logging
from typing import Callable, Dict, List, Any


class EventBus:
    """
    Implements the PubSub (Publish/Subscribe) pattern for communication between different
    packages in GLHE apart from object-to-object transfer.
    You subscribe a function that takes the event, and publish the events themselves.
    """

    topics: Dict[str, List["Subscription"]] = {}

    def Publish(self, published_event: Any) -> None:
        """
        Publishes an event to all subscribers of the event's type.

        :param published_event: An instance of an event, typically an object of a specific class.
        """
        event_type = (
            published_event.__class__.__name__
        )  # Use the event's class name as the event type.
        if event_type in self.topics:
            subscriber_list = self.topics[event_type]
            for subscriber in subscriber_list:
                subscriber.callback(
                    published_event
                )  # Call each subscriber's callback with the event.

    def Subscribe(
        self, event_type: str, callback: Callable[[Any], None]
    ) -> "Subscription":
        """
        Subscribes a callback to an event type.

        :param event_type: The name of the event type (typically the event class name).
        :param callback: A function that takes the event as a parameter.
        :return: A Subscription object representing the subscription.
        """
        new_subscription = Subscription(callback)
        if event_type not in self.topics:
            self.topics[event_type] = (
                []
            )  # Create a new list for the event type if it doesn't exist.
        self.topics[event_type].append(
            new_subscription
        )  # Add the new subscription to the list.
        return new_subscription

    def Unsubscribe(self, subscription: "Subscription") -> None:
        """
        Unsubscribes a callback from an event type.

        :param subscription: The subscription object to be removed.
        """
        event_type = (
            subscription.callback.__class__.__name__
        )  # Get the event type from the callback.
        if event_type in self.topics:
            self.topics[event_type].remove(
                subscription
            )  # Remove the subscription from the event type list.

        else:
            logger = logging.getLogger(self.__class__.__name__)
            logger.info("Subscription did not exist at this time.")


class Subscription:
    """
    Represents a subscription to a specific event type.
    Holds the callback function that will be called when the event is published.
    """

    callback: Callable[[Any], None]

    def __init__(self, _callback: Callable[[Any], None]) -> None:
        """
        Initializes the Subscription object.

        :param _callback: The callback function that will be called when the event occurs.
        """
        self.callback = _callback

    def __del__(self):
        """
        Unsubscribes the callback from the EventBus when the Subscription object is destroyed.
        """
        EventBus().Unsubscribe(self)
