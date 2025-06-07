"use client"

import type { Message } from "../api/chatApi"

interface ChatMessageProps {
  message: Message
  isCurrentUser: boolean
}

export default function ChatMessage({ message, isCurrentUser }: ChatMessageProps) {
  const formatTime = (timestamp: number | string | undefined) => {
    if (timestamp === undefined) {
      return "--:--"
    }
    try {
      const date = new Date(typeof timestamp === "string" ? Date.parse(timestamp) : timestamp * 1000)
      return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    } catch {
      return "--:--"
    }
  }

  const formatDate = (timestamp: number) => {
    const date = new Date(timestamp * 1000)
    return date.toLocaleDateString()
  }

  return (
    <div className={`flex ${isCurrentUser ? "justify-end" : "justify-start"} mb-6 group animate-fade-in`}>
      {!isCurrentUser && (
        <div className="flex-shrink-0 mr-4">
          <div className="h-12 w-12 rounded-2xl bg-secondary-background border border-white/20 flex items-center justify-center text-foreground font-bold text-sm shadow-lg backdrop-blur-sm">
            {message.user_id.substring(0, 2).toUpperCase()}
          </div>
        </div>
      )}

      <div
        className={`max-w-[75%] rounded-3xl px-6 py-4 shadow-xl backdrop-blur-xl border transition-all duration-300 group-hover:scale-105 ${
          isCurrentUser
            ? "bg-accent-teal text-white rounded-tr-lg border-accent-teal/20 shadow-accent-teal/20"
            : "bg-secondary-background text-foreground rounded-tl-lg border-white/20 shadow-white/10"
        }`}
      >
        {!isCurrentUser && <div className="font-bold text-sm mb-2 text-secondary-text">{message.user_id}</div>}
        <div className="whitespace-pre-wrap break-words text-lg leading-relaxed font-medium">{message.content}</div>
        <div className={`text-sm ${isCurrentUser ? "text-white/80" : "text-secondary-text"} text-right mt-3 font-medium`}>
          {formatDate(message.timestamp)} {formatTime(message.timestamp)}
        </div>
      </div>

      {isCurrentUser && (
        <div className="flex-shrink-0 ml-4">
          <div className="h-12 w-12 rounded-2xl bg-accent-teal flex items-center justify-center text-white font-bold text-sm shadow-lg">
            {message.user_id.substring(0, 2).toUpperCase()}
          </div>
        </div>
      )}
    </div>
  )
}
