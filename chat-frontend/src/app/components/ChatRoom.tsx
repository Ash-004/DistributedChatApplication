'use client';

import { useState, useEffect, useRef } from 'react';
import { Message, MessageCreate, Room, fetchMessages, sendMessage } from '../api/chatApi';
import ChatMessage from './ChatMessage';

interface ChatRoomProps {
  room: Room;
  userId: string;
}

export default function ChatRoom({ room, userId }: ChatRoomProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [newMessage, setNewMessage] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isSending, setIsSending] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Load messages when room changes
  useEffect(() => {
    if (room) {
      loadMessages();
      
      // Poll for new messages every 3 seconds
      const intervalId = setInterval(loadMessages, 3000);
      return () => clearInterval(intervalId);
    }
  }, [room.id]);

  // Scroll to bottom when messages change
  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const loadMessages = async () => {
    if (!room) return;
    
    try {
      setIsLoading(true);
      const messageList = await fetchMessages(room.id);
      setMessages(messageList);
      setError(null);
    } catch (error) {
      console.error(`Failed to load messages for room ${room.id}:`, error);
      setError('Failed to load messages. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!newMessage.trim() || !room) return;
    
    try {
      setIsSending(true);
      const messageData: MessageCreate = {
        room_id: room.id,
        user_id: userId,
        content: newMessage.trim()
      };
      
      await sendMessage(messageData);
      setNewMessage('');
      
      // Immediately load messages to show the new message
      await loadMessages();
    } catch (error) {
      console.error('Failed to send message:', error);
      setError('Failed to send message. Please try again.');
    } finally {
      setIsSending(false);
    }
  };

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  return (
    <div className="flex flex-col h-full">
      <div className="bg-white border-b border-gray-200 p-4">
        <h2 className="font-semibold text-xl">{room.name}</h2>
      </div>
      
      <div className="flex-1 overflow-y-auto p-4 bg-gray-50">
        {isLoading && messages.length === 0 ? (
          <div className="flex justify-center items-center h-20">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
          </div>
        ) : messages.length === 0 ? (
          <div className="text-center text-gray-500 py-8">
            No messages yet. Be the first to say something!
          </div>
        ) : (
          <div>
            {messages.map((message) => (
              <ChatMessage 
                key={message.id}
                message={message}
                isCurrentUser={message.user_id === userId}
              />
            ))}
            <div ref={messagesEndRef} />
          </div>
        )}
        
        {error && (
          <div className="bg-red-100 text-red-700 p-3 rounded mb-4">
            {error}
          </div>
        )}
      </div>
      
      <div className="p-4 bg-white border-t border-gray-200">
        <form onSubmit={handleSendMessage} className="flex gap-2">
          <input
            type="text"
            value={newMessage}
            onChange={(e) => setNewMessage(e.target.value)}
            placeholder="Type your message..."
            className="flex-1 p-2 border rounded"
            disabled={isSending}
          />
          <button
            type="submit"
            disabled={!newMessage.trim() || isSending}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded disabled:bg-blue-400"
          >
            {isSending ? (
              <span className="flex items-center">
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Sending
              </span>
            ) : 'Send'}
          </button>
        </form>
      </div>
    </div>
  );
}