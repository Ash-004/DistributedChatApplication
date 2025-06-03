'use client';

import { Message } from '../api/chatApi';

interface ChatMessageProps {
  message: Message;
  isCurrentUser: boolean;
}

export default function ChatMessage({ message, isCurrentUser }: ChatMessageProps) {
  // Format timestamp
  const formatTime = (timestamp: number) => {
    const date = new Date(timestamp * 1000);
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  return (
    <div 
      className={`flex ${isCurrentUser ? 'justify-end' : 'justify-start'} mb-4`}
    >
      <div 
        className={`max-w-[70%] rounded-lg px-4 py-2 ${
          isCurrentUser 
            ? 'bg-blue-600 text-white rounded-tr-none' 
            : 'bg-gray-200 text-gray-800 rounded-tl-none'
        }`}
      >
        {!isCurrentUser && (
          <div className="font-semibold text-xs mb-1">{message.user_id}</div>
        )}
        <div>{message.content}</div>
        <div className={`text-xs ${isCurrentUser ? 'text-blue-100' : 'text-gray-500'} text-right mt-1`}>
          {formatTime(message.timestamp)}
        </div>
      </div>
    </div>
  );
}