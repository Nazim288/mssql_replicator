package com.gpb.replication.mssql.service;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class CustomAuthenticationEntryPoint implements AuthenticationEntryPoint {

    @Override
    public void commence(HttpServletRequest request,
                         HttpServletResponse response,
                         AuthenticationException authException) throws IOException {

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        String usernameHeader = request.getHeader("Authorization");
        String endpoint = request.getRequestURI();
        String clientIp = request.getRemoteAddr();

        if (authException instanceof BadCredentialsException) {
            log.warn("Ошибка авторизации: неправильный логин или пароль. " +
                    "Endpoint: {}, IP: {}, Header: {}", endpoint, clientIp, usernameHeader);

            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("""
                {"error": "Invalid username or password"}
            """);

        } else if (authException instanceof DisabledException) {
            log.warn("Пользователь заблокирован. Endpoint: {}, IP: {}", endpoint, clientIp);

            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            response.getWriter().write("""
                {"error": "User account is disabled"}
            """);

        } else {
            log.error("Ошибка аутентификации: {}. Endpoint: {}, IP: {}",
                    authException.getMessage(), endpoint, clientIp);

            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("""
                {"error": "Authentication failed"}
            """);
        }
    }
}
